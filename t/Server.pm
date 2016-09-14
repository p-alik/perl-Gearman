package t::Server;
use strict;
use warnings;
use base qw/Exporter/;
use Test::TCP;
our @EXPORT = qw/
    new_server
    /;

sub new_server {
    my ($bin, $host, $debug) = @_;
    my $s = Test::TCP->new(
        host => $host,
        code => sub {
            my $port = shift;
            my %args
                = ("--port" => $port, $debug ? ("--verbose" => "DEBUG") : ());

            exec $bin, %args;
            die "cannot execute $bin: $!";
        },
    );

    return $s;
} ## end sub new_server
1;
