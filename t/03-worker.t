use strict;
use warnings;

# OK gearmand v1.0.6

use File::Which qw/ which /;
use Net::EmptyPort qw/ empty_port /;
use Test::More;
use Test::Timer;
use Test::Exception;
use t::Server ();

my $mn = "Gearman::Worker";
use_ok($mn);

can_ok(
    $mn, qw/
        _get_js_sock
        _on_connect
        _register_all
        _set_ability
        job_servers
        register_function
        reset_abilities
        uncache_sock
        unregister_function
        work
        /
);

subtest "new", sub {
    my $w = new_ok($mn);
    isa_ok($w, 'Gearman::Objects');

    is(ref($w->{$_}), "HASH", "$_ is a hash ref") for qw/
        sock_cache
        last_connect_fail
        down_since
        can
        timeouts
        /;
    ok($w->{client_id} =~ /^\p{Lowercase}+$/, "client_id");

    throws_ok {
        local $ENV{GEARMAN_WORKER_USE_STDIO} = 1;
        $mn->new();
    }
    qr/Unable to initialize connection to gearmand/,
        "GEARMAN_WORKER_USE_STDIO env";
};

subtest "register_function", sub {
    my $w = new_ok($mn);
    my ($tn, $to) = qw/foo 2/;
    my $cb = sub {1};

    ok($w->register_function($tn => $cb), "register_function($tn)");

    time_ok(
        sub {
            $w->register_function($tn, $to, $cb);
        },
        $to,
        "register_function($to, cb)"
    );
};

subtest "reset_abilities", sub {
    my $w = new_ok($mn);
    $w->{can}->{x}      = 1;
    $w->{timeouts}->{x} = 1;

    ok($w->reset_abilities());

    is(keys %{ $w->{can} },      0);
    is(keys %{ $w->{timeouts} }, 0);
};

subtest "work", sub {
    my $w = new_ok($mn);

    time_ok(
        sub {
            $w->work(stop_if => sub { pass "work stop if"; });
        },
        12,
        "stop if timeout"
    );
};

subtest "_get_js_sock", sub {
    my $w = new_ok($mn);

    is($w->_get_js_sock(), undef, "_get_js_sock() returns undef");

    $w->{parent_pipe} = rand(10);
    my $hp = join ':', "127.0.0.1", empty_port();

    is($w->_get_js_sock($hp), $w->{parent_pipe}, "parent_pipe");

    delete $w->{parent_pipe};
    is($w->_get_js_sock($hp), undef, "_get_js_sock($hp) undef");

    my $gts = t::Server->new();
SKIP: {
        $gts || skip $t::Server::ERROR, 4;

        my $job_server = $gts->job_servers();
        $job_server || skip "couldn't start ", $gts->bin(), 4;

        ok($w->job_servers($job_server));

        $hp                          = $w->job_servers()->[0];
        $w->{last_connect_fail}{$hp} = 1;
        $w->{down_since}{$hp}        = 1;

        isa_ok($w->_get_js_sock($hp, on_connect => sub {1}), "IO::Socket::IP");
        is($w->{last_connect_fail}{$hp}, undef);
        is($w->{down_since}{$hp},        undef);
    } ## end SKIP:
};

subtest "_on_connect-_set_ability", sub {
    my $w = new_ok($mn);
    my $m = "foo";

    is($w->_on_connect(), undef);

    is($w->_set_ability(), 0);
    is($w->_set_ability(undef, $m), 0);
    is($w->_set_ability(undef, $m, 2), 0);
};

done_testing();

