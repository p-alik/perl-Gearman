use strict;
use warnings;
use Test::More;
use Test::Differences qw(eq_or_diff);
use IO::Socket::SSL ();

my $mn = "Gearman::Objects";
use_ok($mn);

can_ok(
    $mn, qw/
        _property
        _sock_cache
        canonicalize_job_servers
        debug
        job_servers
        prefix
        set_job_servers
        sock_nodelay
        socket
        /
);

subtest "job servers", sub {
    {
        # scalar
        my $host = "foo";
        my $c    = new_ok(
            $mn,
            [job_servers => $host],
            "Gearman::Objects->new(job_servers => $host)"
        );

        is(1, $c->{js_count}, "js_count=1");
        ok(my @js = $c->job_servers(), "job_servers");
        is(scalar(@js), 1, "job_servers count");

        eq_or_diff($js[0], @{ $c->canonicalize_job_servers($host) }[0],
            "job_servers=$host");
        is($js[0], join(':', $host, 4730), "$host:4730");
    }

    {
        # hash reference
        my $j = { host => "foo", port => 123 };
        my $c = new_ok(
            $mn,
            [job_servers => $j],
            "Gearman::Objects->new(job_servers => hash reference)"
        );

        is(1, $c->{js_count}, "js_count=1");
        ok(my @js = $c->job_servers(), "job_servers");
        is(scalar(@js), 1, "job_servers count");

        eq_or_diff($js[0], @{ $c->canonicalize_job_servers($j) }[0],
            "job_servers");
    }

    {
        # mix scalar and hash reference
        my @servers = (
            qw/
                foo:12345
                bar:54321
                /, { host => "abc", "port" => 123 }
        );

        my $c = new_ok($mn, [job_servers => [@servers]],);

        is(scalar(@servers), $c->{js_count}, "js_count=" . scalar(@servers));
        ok(my @js = $c->job_servers, "job_servers");
        isa_ok($js[$#servers], "HASH");
        for (my $i = 0; $i <= $#servers; $i++) {
            eq_or_diff(@{ $c->canonicalize_job_servers($servers[$i]) }[0],
                $js[$i], "canonicalize_job_servers($servers[$i])");
        }
    }
};

subtest "debug", sub {
    my $c = new_ok($mn, [debug => 1]);
    is($c->debug(),  1);
    is($c->debug(0), 0);
    $c = new_ok($mn);
    is($c->debug(),  undef);
    is($c->debug(1), 1);
};

subtest "prefix", sub {
    my $p = "foo";
    my $c = new_ok($mn, [prefix => $p]);
    is($c->prefix(),      $p);
    is($c->prefix(undef), undef);
    $c = new_ok($mn);
    is($c->prefix(),   undef);
    is($c->prefix($p), $p);
};

subtest "socket", sub {

    #TODO skip if can not resolve google.com
    my $host = "google.com";
    my $to   = int(rand(5)) + 1;
    my $js   = {
        use_ssl   => 1,
        socket_cb => sub { my ($hr) = @_; $hr->{Timeout} = $to; },
        host      => $host,
        port      => 443
    };

    my $c = new_ok($mn);
SKIP: {
        my $sock = $c->socket($js);
        $sock
            || skip
            "failed connect to $host:$js->{port} or ssl handshake: $!, $IO::Socket::SSL::SSL_ERROR",
            2;
        isa_ok($sock, "IO::Socket::SSL");
        is($sock->timeout, $to, "ssl socket callback");
    } ## end SKIP:

    $to = int(rand(5)) + 1;
    $js->{port} = 80;

SKIP: {
        my $sock = $c->socket($js);
        $sock || skip "failed connect to $js: $!", 2;
        isa_ok($sock, "IO::Socket::IP");
        is($sock->timeout, $to, "ssl socket callback");
    } ## end SKIP:
};

subtest "sock cache", sub {
    my $c = new_ok($mn);
    isa_ok($c->{sock_cache}, "HASH");
    is(keys(%{ $c->{sock_cache} }), 0);
    my ($k, $v) = qw/x y/;

    # nothing in cache
    is($c->_sock_cache($k), undef);

    # set cache x = y
    is($c->_sock_cache($k, $v), $v);
    is(keys(%{ $c->{sock_cache} }), 1);

    # delete x
    is($c->_sock_cache($k, $v, 1), $v);
    is(keys(%{ $c->{sock_cache} }), 0);

    $k = { host => $k, port => 123 };
    is($c->_sock_cache($k, $v), $v);
    is(keys(%{ $c->{sock_cache} }), 1);
};

done_testing();
