package t::Server;
use strict;
use warnings;
use base qw/Exporter/;
use Test::TCP;
our @EXPORT = qw/
    new_server
    /;

sub new_server {
    my ($bin, $host) = @_;
    my $s = Test::TCP->new(
        host => $host,
        code => sub {
            my $port = shift;
            exec $bin, "--port" => $port;    #, "--verbose=INFO";
            die "cannot execute $bin: $!";
        },
    );

    return $s;
} ## end sub new_server
1;
