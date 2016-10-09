package t::Server;
use strict;
use warnings;

use base qw/Exporter/;
use fields qw/
    _bin
    _ip
    _servers
    /;

use File::Which ();
use Test::TCP;

use vars qw/
    $ERROR
    /;

our @EXPORT = qw/
    $ERROR
    /;

sub new {
    my ($self) = @_;
    unless (ref $self) {
        $self = fields::new($self);
    }

    if ($ENV{GEARMAND_ADDR}) {

    }
    else {
        my $daemon = "gearmand";
        my $bin = $ENV{GEARMAND_PATH} || File::Which::which($daemon);

        unless ($bin) {
            $ERROR = "Can't find $daemon to test with";
        }
        elsif (!-X $bin) {
            $ERROR = "$bin is not executable";
        }

        $ERROR && return;

        $self->{_ip}      = $ENV{GEARMAND_IP} || "127.0.0.1";
        $self->{_bin}     = $bin;
        $self->{_servers} = {};
    } ## end else [ if ($ENV{GEARMAND_ADDR...})]

    return $self;
} ## end sub new

sub job_servers {
    my ($self) = @_;
    my $s = Test::TCP->new(
        host => $self->{_ip},
        code => sub {
            my $port = shift;
            my %args = (
                "--port" => $port,
                $ENV{GEARMAND_DEBUG} ? ("--verbose" => "DEBUG") : ()
            );

            exec $self->bin(), %args;

            # $ERROR = sprintf "cannot execute %s: $!", $self->bin;
        },
    );

    if ($ERROR) {
        undef($s);
        return;
    }

    $self->{_servers}->{ $s->port } = $s;
    return join ':', $self->host, $s->port;
} ## end sub job_servers

sub bin {
    return shift->{_bin};
}

sub host {
    return shift->{_ip};
}

1;
