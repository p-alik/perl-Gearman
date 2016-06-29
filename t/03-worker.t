use strict;
use warnings;
use Test::More;
use Test::Timer;

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
    my $w = new_ok($mn, [job_servers => [@js]]);
    isa_ok($w, 'Gearman::Objects');

    is(ref($w->{sock_cache}),        "HASH");
    is(ref($w->{last_connect_fail}), "HASH");
    is(ref($w->{down_since}),        "HASH");
    is(ref($w->{can}),               "HASH");
    is(ref($w->{timeouts}),          "HASH");
    ok($w->{client_id} =~ /^\p{Lowercase}+$/);
};

subtest "register_function", sub {
    my $w = new_ok($mn, [job_servers => [@js], debug => $debug]);
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

subtest "work", sub {

    # $ENV{AUTHOR_TESTING} || plan skip_all => 'without $ENV{AUTHOR_TESTING}';
    # $ENV{GEARMAN_SERVERS}
    #     || plan skip_all => 'without $ENV{GEARMAN_SERVERS}';

    my $w = new_ok($mn, [job_servers => [@js]]);

    time_ok(
        sub {
            $w->work(stop_if => sub { pass "work stop if"; });
        },
        12,
        "stop if timeout"
    );
};

subtest "_get_js_sock", sub {
    my $w = new_ok($mn, [job_servers => [@js], debug => $debug]);
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

done_testing();
