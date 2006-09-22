#$Id: Oracle.pm,v 1.9 2006/08/11 17:06:14 jef539 Exp $
package DBIx::Fun::Oracle;
use strict;
use DBD::Oracle;
use Carp ();

use base 'DBIx::Fun';
our $VERSION = '0.00';

# proc
# package . proc
# schema  . package . proc
# syn
# syn     . proc
# schema  . syn
# schema  . syn     . proc

sub _lookup_synonym {

    # print STDERR "LOOKUP SYNONYM @_\n";
    my ( $self, $syn ) = @_;
    my @path = $self->_path;
    return if @path > 1;

    my $schema = $path[0];

    my ( $sth, $count );

    if ( defined $schema ) {
        $sth = $self->dbh->prepare(<<EOQ);
        SELECT COUNT(*)
        FROM ALL_SYNONYMS
        WHERE SYNONYM_NAME = UPPER(?) 
        AND OWNER = UPPER(?)
EOQ
        $sth->execute( $syn, $schema );
        ($count) = $sth->fetchrow_array;
        $sth->finish;
    }
    else {
        $sth = $self->dbh->prepare(<<EOQ);
        SELECT TABLE_OWNER, TABLE_NAME
        FROM USER_SYNONYMS
        WHERE SYNONYM_NAME = UPPER(?) 
EOQ
        $sth->execute($syn);
        ($count) = $sth->fetchrow_array;
        $sth->finish;

        if ( not $count ) {
            $sth = $self->dbh->prepare(<<EOQ);
            SELECT TABLE_OWNER, TABLE_NAME
            FROM ALL_SYNONYMS
            WHERE SYNONYM_NAME = UPPER(?) 
            AND OWNER = 'PUBLIC'
EOQ
            $sth->execute($syn);
            ($count) = $sth->fetchrow_array;
            $sth->finish;
        }
    }
    return $count;
}

sub _lookup_user {

    # print STDERR "LOOKUP USER @_\n";
    my ( $self, $user ) = @_;
    return if $self->_path;

    my $sth = $self->dbh->prepare(<<EOQ);
    SELECT COUNT(*)
    FROM ALL_USERS
    WHERE USERNAME = UPPER(?)
EOQ
    $sth->execute($user);
    my ($count) = $sth->fetchrow_array;
    $sth->finish;

    return $count;
}

sub _lookup_package {

    # print STDERR "LOOKUP PACKAGE @_\n";
    my ( $self, $package ) = @_;
    my @path = $self->_path;
    return if @path > 1;

    my $owner = $path[0];
    if ( defined $owner ) {
        my $sth = $self->dbh->prepare(<<EOQ);
       SELECT COUNT(*)
         FROM ALL_OBJECTS
        WHERE OBJECT_NAME = UPPER(?)
          AND OWNER = UPPER(?)
          AND OBJECT_TYPE = 'PACKAGE'
EOQ
        $sth->execute( $package, $owner );
        my ($count) = $sth->fetchrow_array;
        $sth->finish;
        return $count;
    }
    my $sth = $self->dbh->prepare(<<EOQ);
       SELECT COUNT(*)
         FROM USER_OBJECTS
        WHERE OBJECT_NAME = UPPER(?)
          AND OBJECT_TYPE = 'PACKAGE'
EOQ
    $sth->execute($package);
    my ($count) = $sth->fetchrow_array;
    $sth->finish;
    return $count;
}

sub _lookup_procedure {

    # print STDERR "LOOKUP PROCEDURE @_\n";
    my ( $self, $proc ) = @_;
    my @path = $self->_path;
    return unless _path_ok( @path, $proc );

    my $path = uc join '.', map { qq("$_") } @path, $proc;

    my $ref = eval { $self->_describe_procedure($path) };

    # retry if invalid
    $ref = eval { $self->_describe_procedure($path) }
      if $@ =~ /ORA-20003/;

    # Carp::croak $@ if $@ and $@ !~ /ORA-06564|ORU-10035|ORU-10032/;

    return $ref;
}

sub _lookup_standard_procedure {

    # print STDERR "LOOKUP STANDARD PROCEDURE @_\n";
    my ( $self, $proc ) = @_;
    my @path = $self->_path;
    return if @path != 0 and @path != 2;
    return
      if @path == 2
      and ( uc( $path[0] ) ne 'SYS' or uc( $path[0] ) ne 'STANDARD' );

    local $self->dbh->{FetchHashKeyName} = 'NAME_lc';

    my $ref = $self->dbh->selectall_arrayref( <<EOQ, {}, $proc );
       SELECT OVERLOAD, SEQUENCE, POSITION, 
         ARGUMENT_NAME, DATA_LEVEL, IN_OUT, DATA_TYPE
         FROM ALL_ARGUMENTS
        WHERE OBJECT_NAME = UPPER(?) 
        AND OWNER = 'SYS'
        AND PACKAGE_NAME = 'STANDARD'
        ORDER BY OBJECT_NAME, OVERLOAD, SEQUENCE
EOQ
    return if not $ref or not @$ref;

    my @over;

    for my $row (@$ref) {
        my ( $overload, $sequence, $position, $name, $level, $in_out, $type ) =
          @$row;

        my $ref = @over[ $overload || 0 ] ||= {};

        $ref = $ref->{pos}[-1] while ( $level-- > 0 );
        $ref->{pos} ||= [undef];

        next if not $type;

        $ref->{pos}[$position] = {
            name => $name,
            type => $type,
            in   => scalar( $in_out =~ /in/i ),
            out  => scalar( $in_out =~ /out/i ),
        };

        $ref->{name}{ uc $name } = $ref->{pos}[$position]
          if defined $name
          and length $name;
    }

    return \@over;
}

sub _lookup {
    my ( $self, $name ) = @_;
    return unless _path_ok($name);

    # HASH = context
    # CODE = procedure

    my $ref = $self->{cache}{$name};

    if ( not $self->{cache}{$name} ) {

        my $obj = $self->_lookup_procedure($name);

        if ( not $obj ) {
            $obj = $self->_lookup_package($name);
        }
        if ( not $obj ) {
            $obj = $self->_lookup_synonym($name);
        }
        if ( not $obj ) {
            $obj = $self->_lookup_user($name);
        }

        if ( not $obj ) {
            $obj = $self->_lookup_standard_procedure($name);
        }

        if ( ref $obj ) {
            $self->{cache}{$name} = $self->_make_procedure( $name, $obj );
        }
        elsif ($obj) {
            $self->{cache}{$name} =
              { name => $name, path => [ $self->_path, $name ], cache => {} };
        }

        $ref = $self->{cache}{$name};
    }
    return $ref if ref($ref) eq 'CODE';
    return $self->context(%$ref) if $ref;
    return \&_fetch_variable;
}

# ALL_ARGUMENTS does not properly return defaults
# so we call DBMS_DESCRIBE.DESCRIBE_PROCEDURE

my %datatype = (
    0,   undef,
    1,   'VARCHAR2',
    2,   'NUMBER',
    3,   'NATIVE INTEGER',
    8,   'LONG',
    9,   'VARCHAR',
    11,  'ROWID',
    12,  'DATE',
    23,  'RAW',
    24,  'LONG RAW',
    29,  'BINARY_INTEGER',
    69,  'ROWID',
    96,  'CHAR',
    102, 'REF CURSOR',
    104, 'UROWID',
    105, 'MLSLABEL',
    106, 'MLSLABEL',
    110, 'REF',
    111, 'REF',
    112, 'CLOB',
    113, 'BLOB',
    114, 'BFILE',
    115, 'CFILE',
    121, 'OBJECT',
    122, 'TABLE',
    123, 'VARRAY',
    178, 'TIME',
    179, 'TIME WITH TIME ZONE',
    180, 'TIMESTAMP',
    181, 'TIMESTAMP WITH TIME ZONE',
    231, 'TIMESTAMP WITH LOCAL TIME ZONE',
    182, 'INTERVAL YEAR TO MONTH',
    183, 'INTERVAL DAY TO SECOND',
    250, 'PL/SQL RECORD',
    251, 'PL/SQL TABLE',
    252, 'PL/SQL BOOLEAN',
);
my %inout = ( 0, 'IN', 1, 'OUT', 2, 'IN/OUT' );

sub _describe_procedure {
    my ( $self, $path ) = @_;

    # print STDERR "DESCRIBE $path\n";
    my $sql = <<EOQ;
DECLARE
   overload        DBMS_DESCRIBE.NUMBER_TABLE;
   position        DBMS_DESCRIBE.NUMBER_TABLE;
   level           DBMS_DESCRIBE.NUMBER_TABLE;
   argument_name   DBMS_DESCRIBE.VARCHAR2_TABLE;
   datatype        DBMS_DESCRIBE.NUMBER_TABLE;
   default_value   DBMS_DESCRIBE.NUMBER_TABLE;
   in_out          DBMS_DESCRIBE.NUMBER_TABLE;
   length          DBMS_DESCRIBE.NUMBER_TABLE;
   precision       DBMS_DESCRIBE.NUMBER_TABLE;
   scale           DBMS_DESCRIBE.NUMBER_TABLE;
   radix           DBMS_DESCRIBE.NUMBER_TABLE;
   spare           DBMS_DESCRIBE.NUMBER_TABLE; 
   
   buffer VARCHAR2(32000) := '';
   i NUMBER;
BEGIN

DBMS_DESCRIBE.DESCRIBE_PROCEDURE(
   :input         , NULL, NULL ,
   overload       ,
   position       ,
   level          ,
   argument_name  ,
   datatype       ,
   default_value  ,
   in_out         ,
   length         ,
   precision      ,
   scale          ,
   radix          ,
   spare          ); 
  
  i := overload.FIRST;
  WHILE i IS NOT NULL
  LOOP
      buffer := buffer || 
                overload(i)     || ',' ||
                position(i)     || ',' ||
                level(i)        || ',' ||
                argument_name(i)    || ',' ||
                datatype(i) || ',' ||
                in_out(i) || ',' ||
                default_value(i) || chr(10);
       i := overload.next(i);
  END LOOP;

  :output := buffer;
END;
EOQ
    local $self->dbh->{RaiseError}  = 1;
    local $self->dbh->{HandleError} = undef;
    local $self->dbh->{PrintError}  = 0;

    my $sth = $self->dbh->prepare($sql);
    my $output;

    $sth->bind_param( ":input", $path );
    $sth->bind_param_inout( ":output", \$output, 32767 );
    $sth->execute();

    my @lines = split /\n/, $output;
    my @over;

    for my $line (@lines) {
        my ( $overload, $position, $level, $name, $type, $inout, $default ) =
          split /,\s*/, $line;

        my $ref = @over[ $overload || 0 ] ||= {};

        $ref = $ref->{pos}[-1] while ( $level-- > 0 );
        $ref->{pos} ||= [undef];

        next if not $type;

        my $in_out = $inout{ $inout || 0 } || $inout;

        $ref->{pos}[$position] = {
            name    => $name,
            type    => $datatype{ $type || 0 } || $type,
            default => $default,
            in  => scalar( $in_out =~ /in/i ),
            out => scalar( $in_out =~ /out/i ),
        };

        $ref->{name}{ uc $name } = $ref->{pos}[$position]
          if defined $name
          and length $name;
    }

    return \@over;
}

use Date::Parse ();
use POSIX       ();

my %typemap = (
    'CHAR' => {
        map_out => sub { local $_ = $_[0]; s/^(.+?) +\z/$1/s if defined; $_ },
    },
    'PL/SQL BOOLEAN' => {
        map_in => sub { $_[0] ? 1 : 0 },
        declare =>
          "%s BOOLEAN := CASE NVL(%s,0) WHEN 0 THEN FALSE ELSE TRUE END;\n",
        assign => "%s := CASE %s WHEN TRUE THEN 1 ELSE 0 END;\n",
    },

    'DATE' => {
        map_in => sub {
            my $date = shift;
            return undef unless defined $date;
            $date = Date::Parse::str2time($date)
              if $date !~ /^[\-+]?\d*\.?\d*\s*$/;
            return POSIX::strftime( "%Y-%m-%d %H:%M:%S", localtime $date );
        },
        declare => "%s DATE := TO_DATE(%s, 'YYYY-MM-DD HH24:MI:SS');\n",
        assign  => "%s := TO_CHAR(%s, 'YYYY-MM-DD HH24:MI:SS');\n",
    },

    'TIMESTAMP WITH TIME ZONE' => {
        map_in => sub {
            my $date = shift;
            return undef unless defined $date;
            $date = Date::Parse::str2time($date)
              if $date !~ /^[\-+]?\d*\.?\d*\s*$/;
            my $fraction = '000';
            $fraction = $1 if $date =~ /\.(\d+)/;
            return POSIX::strftime( "%Y-%m-%d %H:%M:%S.$fraction%z",
                localtime $date );
        },
        declare =>
"%s TIMESTAMP WITH TIME ZONE := TO_TIMESTAMP_TZ(%s,'YYYY-MM-DD HH24:MI:SS.FFTZH:TZM');\n",
        assign => "%s := TO_CHAR(%s, 'YYYY-MM-DD HH24:MI:SS.FFTZH:TZM');\n",
    },
);

sub _make_procedure {
    my ( $self, $name, $proc_ref ) = @_;
    return unless _path_ok( $self->_path, $name );
    my $path = uc join '.', map { qq("$_") } $self->_path, $name;

    my $sub = sub {
        my ( $self, $name, @args ) = @_;
        my %named;
        %named = %{ pop @args } if @args and ref( $args[-1] ) eq 'HASH';
        my %keys = map { uc($_) => $_ } keys %named;

        my ( $func, $proc );

        for my $i ( 0 .. $#$proc_ref ) {
            my $spec = $proc_ref->[$i];
            next if not $spec;

            my $argc = $#{ $spec->{pos} };

            # Too many input arguments ?
            next if @args > $argc;

            # Named input argument not found in spec?
            next if grep { not exists $spec->{name}{ uc $_ } } keys %named;

            # Spec arguments beyond input arguments
            #   have no default and are not in named input args?

            next
              if
              grep { not $_->{default} and not exists $keys{ uc $_->{name} } }
              @{ $spec->{pos} }[ @args + 1 .. $argc ];

            # All OUT args are refs
            next
              if grep { $spec->{pos}[ $_ + 1 ]{out} and not ref $args[$_] }
              ( 0 .. $#args );

            # All OUT named args are refs
            next
              if grep { $spec->{name}{ uc $_ }{out} and not ref $named{$_} }
              keys %named;

            if ( $spec->{pos}[0] ) {
                $func ||= $spec;
            }
            else {
                $proc ||= $spec;
            }
        }

        # choose the procedure unless we want a result

        $proc = $func if !$proc or ( $func and defined wantarray );

        # if there is no matching signature, punt
        if ( !$proc ) {
            for my $spec (@$proc_ref) {
                next unless $spec;
                for my $i ( 0 .. $#{ $spec->{pos} } ) {
                    $proc->{pos}[$i] ||= $spec->{pos}[$i];
                }
                for my $key ( keys %{ $spec->{name} } ) {
                    $proc->{name}{$key} = $spec->{name}{$key};
                }
            }
        }

        my ( @declare, @bind, @assign );

        # make ret arg 0
        my $ret = undef;
        unshift @args, \$ret;

        # put named args on args
        my @keys;
        for my $key ( keys %named ) {
            push @args, $named{$key};
            $keys[$#args] = $key;
        }

        my @p;
        for my $i ( 0 .. $#args ) {
            $bind[$i] = ":p_$i";

            my $p = $p[$i] =
              defined( $keys[$i] )
              ? $proc->{name}{ uc $keys[$i] }
              : $proc->{pos}[$i];

            if ( $p and my $typemap = $typemap{ $p->{type} } ) {
                my $ref = ref( $args[$i] ) ? $args[$i] : \$args[$i];
                $$ref = $typemap->{map_in}->($$ref)
                  if $typemap->{map_in};

                push @declare, sprintf $typemap->{declare}, "v_$i", ":p_$i"
                  if $typemap->{declare};

                push @assign, sprintf $typemap->{assign}, ":p_$i", "v_$i"
                  if $typemap->{assign}
                  and $p->{out};

                $bind[$i] = "v_$i" if $typemap->{declare};
            }

            $bind[$i] = "$keys[$i] => $bind[$i]" if defined $keys[$i];
        }

        my $return = $proc->{pos}[0] ? "$bind[0] :=" : '';
        my $bind = join ', ', @bind[ 1 .. $#bind ];

        my $sql = <<EOQ;
DECLARE
 @declare
BEGIN
   $return $path( $bind );
 @assign
END;
EOQ
        eval {
            my $sth = $self->dbh->prepare($sql);

            #print STDERR "$sql\n";

            for my $i ( 0 .. $#args ) {
                next if not $i and not $return;
                my $attr = {};
                my $p    = $p[$i];

                $attr = { ora_type => DBD::Oracle::ORA_RSET }
                  if $p->{type} eq 'REF CURSOR';

                $attr = { ora_type => DBD::Oracle::ORA_RAW }
                  if $p->{type} =~ /RAW$/;

                if ( ( $p->{out} || !$i ) and ref $args[$i] ) {
                    $sth->bind_param_inout( ":p_$i", $args[$i], 32767, $attr );
                }
                else {
                    $sth->bind_param( ":p_$i", $args[$i], $attr );
                }
            }

            $sth->execute;

            for my $i ( 0 .. $#args ) {
                my $p = $p[$i];
                next
                  unless $p
                  and $p->{out}
                  and ref $args[$i]
                  and my $typemap = $typemap{ $p->{type} };

                ${ $args[$i] } = $typemap->{map_out}->( ${ $args[$i] } )
                  if $typemap->{map_out};
            }
        };
        Carp::croak $@ if $@;

        # print STDERR "CALLED $path\n";

        return $ret;
    };
    return $sub;
}

sub _fetch_variable {
    my ( $self, $name ) = @_;

    my @path = $self->_path;

    $self->_croak_notfound($name) unless _path_ok( @path, $name );

    my $path = uc join '.', map { qq("$_") } @path, $name;
    my $out;

    local $self->dbh->{RaiseError}  = 1;
    local $self->dbh->{HandleError} = undef;
    local $self->dbh->{PrintError}  = 0;

    eval {
        my $sth = $self->dbh->prepare(<<EOQ);
BEGIN
    :out := $path;
END;
EOQ
        $sth->bind_param_inout( ':out', \$out, 4096 );
        $sth->execute;
    };

    if ($@) {
        $self->_croak_notfound($name) if $@ =~ /PLS-00302|PLS-00201|PLS-00222/;
        Carp::croak $@;
    }

    return $out;
}

sub _path_ok {
    shift if ref( $_[0] ) and UNIVERSAL::isa( $_[0], __PACKAGE__ );
    return unless @_;
    for (@_) {
        return unless defined and /^[A-Za-z][\w\$\#]*\z/;
    }
    return 1;
}

