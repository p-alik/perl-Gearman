use strict;
use warnings;
use Test::More;
use Test::Timer;
use IO::Socket::INET;

my $debug = $ENV{DEBUG};
my @js    = $ENV{GEARMAN_SERVERS} ? split /,/, $ENV{GEARMAN_SERVERS} : ();
my $mn    = "Gearman::Worker";
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
        reset_abilities
        uncache_sock
        unregister_function
        work

        /
);

subtest "new", sub {
    my $w = _w();
    isa_ok($w, 'Gearman::Objects');

    is(ref($w->{sock_cache}),        "HASH");
    is(ref($w->{last_connect_fail}), "HASH");
    is(ref($w->{down_since}),        "HASH");
    is(ref($w->{can}),               "HASH");
    is(ref($w->{timeouts}),          "HASH");
    ok($w->{client_id} =~ /^\p{Lowercase}+$/);
};

subtest "register_function", sub {
    my $w = _w();
    my ($tn, $to) = qw/foo 2/;
    my $cb = sub {
        my ($j) = @_;
        note join(' ', 'work on', $j->handle, explain $j->arg);
        return $j->arg ? $j->arg : 'done';
    };

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
    my $w = _w();
    $w->{can}->{x}      = 1;
    $w->{timeouts}->{x} = 1;

    ok($w->reset_abilities());

    is(keys %{ $w->{can} },      0);
    is(keys %{ $w->{timeouts} }, 0);
};

subtest "work", sub {

    # $ENV{AUTHOR_TESTING} || plan skip_all => 'without $ENV{AUTHOR_TESTING}';
    # $ENV{GEARMAN_SERVERS}
    #     || plan skip_all => 'without $ENV{GEARMAN_SERVERS}';

    my $w = _w();

    time_ok(
        sub {
            $w->work(stop_if => sub { pass "work stop if"; });
        },
        12,
        "stop if timeout"
    );
};

subtest "_get_js_sock", sub {
    my $w = _w();
    is($w->_get_js_sock(), undef);

    $w->{parent_pipe} = rand(10);
    my $hp = "127.0.0.1:9050";

    is($w->_get_js_sock($hp), $w->{parent_pipe});

    delete $w->{parent_pipe};
    is($w->_get_js_sock($hp), undef);

SKIP: {
        @{ $w->job_servers() } || skip 'without $ENV{GEARMAN_SERVERS}', 3;

        my $hp = $w->job_servers()->[0];

        $w->{last_connect_fail}{$hp} = 1;
        $w->{down_since}{$hp}        = 1;
        isa_ok($w->_get_js_sock($hp, on_connect => sub {1}),
            "IO::Socket::INET");

        is($w->{last_connect_fail}{$hp}, undef);
        is($w->{down_since}{$hp},        undef);
    } ## end SKIP:

};

subtest "_on_connect-_set_ability", sub {
    my $w = _w();
    my $m = "foo";

    is($w->_on_connect(), undef);

    is($w->_set_ability(), 0);
    is($w->_set_ability(undef, $m), 0);
    is($w->_set_ability(undef, $m, 2), 0);

    my @js = @{ $w->job_servers() };
    if (@js) {
        my $s = IO::Socket::INET->new(
            PeerAddr => $js[0],
            Timeout  => 1
        );
        is($w->_on_connect($s), 1);

        is($w->_set_ability($s, $m), 1);
        is($w->_set_ability($s, $m, 2), 1);
    } ## end if (@js)
};

done_testing();

sub _w {
    return new_ok($mn, [job_servers => [@js], debug => $debug]);
}
