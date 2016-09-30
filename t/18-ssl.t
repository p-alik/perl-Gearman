use strict;
use warnings;

use Test::More;

BEGIN {
    use IO::Socket::SSL ();
    if (defined($ENV{SSL_DEBUG})) {
        $IO::Socket::SSL::DEBUG = $ENV{SSL_DEBUG};
    }
} ## end BEGIN

{
    my @env = qw/
        AUTHOR_TESTING
        SSL_GEARMAND_ADDR
        SSL_VERIFY_MODE
        SSL_CERT_FILE
        SSL_KEY_FILE
        /;
    my $skip;
    while (my $e = shift @env) {
        defined($ENV{$e}) && next;
        $skip = $e;
        last;
    }
    $skip && plan skip_all => sprintf 'without $ENV{%s}', $skip;
}

my $job_server = $ENV{SSL_GEARMAND_ADDR};

my $ssl_cb = sub {
    my ($hr) = @_;
    $hr->{SSL_verify_mode} = eval "$ENV{SSL_VERIFY_MODE}";
    $hr->{SSL_ca_file}     = $ENV{SSL_CA_FILE};
    $hr->{SSL_cert_file}   = $ENV{SSL_CERT_FILE};
    $hr->{SSL_key_file}    = $ENV{SSL_KEY_FILE};
    return $hr;
};

subtest "client echo request", sub {
    use_ok("Gearman::Client");
    my $client = new_ok(
        "Gearman::Client",
        [
            exceptions    => 1,
            use_ssl       => 1,
            ssl_socket_cb => $ssl_cb,
            job_servers   => [$job_server]
        ]
    );
    ok(my $sock = $client->_get_random_js_sock(), "get socket");
    _echo($sock);
};

subtest "worker echo request", sub {
    use_ok("Gearman::Worker");
    my $worker = new_ok(
        "Gearman::Worker",
        [
            exceptions    => 1,
            use_ssl       => 1,
            ssl_socket_cb => $ssl_cb,
            job_servers   => [$job_server],
            debug         => 0,
        ]
    );

    ok(
        my $sock = $worker->_get_js_sock(
            $worker->job_servers()->[0],
            on_connect => sub {return 1;}
        ),
        "get socket"
    ) || return;

    _echo($sock);
};

sub _echo {
  my ($sock) = @_;
    ok(my $req = Gearman::Util::pack_req_command("echo_req"),
        "prepare echo req");
    my $len = length($req);
    ok(my $rv = $sock->write($req, $len), "write to socket");
    my $err;
    ok(my $res = Gearman::Util::read_res_packet($sock, \$err), "read respose");
    is(ref($res),    "HASH",     "respose is a hash");
    is($res->{type}, "echo_res", "response type");
}

done_testing();

