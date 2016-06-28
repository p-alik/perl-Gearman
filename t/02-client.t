use strict;
use warnings;

use Time::HiRes qw/
    gettimeofday
    tv_interval
    /;

use Test::More;
use Test::Exception;

my $mn = "Gearman::Client";
my @js = $ENV{GEARMAN_SERVERS} ? split /,/, $ENV{GEARMAN_SERVERS} : ();

use_ok($mn);

can_ok(
    $mn, qw/
        _get_js_sock
        _get_random_js_sock
        _get_task_from_args
        _job_server_status_command
        _option_request
        _put_js_sock
        add_hook
        dispatch_background
        do_task
        get_job_server_clients
        get_job_server_jobs
        get_job_server_status
        get_status
        new_task_set
        run_hook
        /
);

my $c = new_ok($mn, [job_servers => [@js]]);

isa_ok($c, "Gearman::Objects");
is($c->{backoff_max},     90, join "->", $mn, "{backoff_max}");
is($c->{command_timeout}, 30, join "->", $mn, "{command_timeout}");
is($c->{exceptions},      0,  join "->", $mn, "{exceptions}");
is($c->{js_count}, scalar(@js), "js_count");
is(keys(%{ $c->{hooks} }),      0, join "->", $mn, "{hooks}");
is(keys(%{ $c->{sock_cache} }), 0, join "->", $mn, "{sock_cache}");

ok(my $r = $c->get_job_server_status, "get_job_server_status");
is(ref($r), "HASH", "get_job_server_status result is a HASH reference");

ok(my $r = $c->get_job_server_clients, "get_job_server_clients");
ok($r = $c->get_job_server_jobs, "get_job_server_jobs");

# throws_ok { $c->get_job_server_clients }
# qr/deprecated because Gearman Administrative Protocol/,
#     "caught deprecated get_job_server_clients exception";

foreach ($c->job_servers()) {
    ok(my $s = $c->_get_js_sock($_), "_get_js_sock($_)");
    isa_ok($s, "IO::Socket::INET");
}

subtest "get_status", sub {
    is($c->get_status(), undef, "get_status()");
    my $h = "localhost:4730";
    is($c->get_status($h), undef, "get_status($h)");
    if (@{ $c->job_servers() }) {
        $h = join "//", @{ $c->job_servers() }[0], "H:foo:5252";
        isa_ok($c->get_status($h), "Gearman::JobStatus", "get_status($h)");
    }
};

my ($tn, $args, $timeout) = qw/
    foo
    bar
    2
    /;

subtest "new_task_set", sub {
    my $h = "new_task_set";
    my $cb = sub { pass("$h cb") };
    ok($c->add_hook($h, $cb), "add_hook($h, cb)");
    is($c->{hooks}->{$h}, $cb, "$h eq cb");
    isa_ok($c->new_task_set(), "Gearman::Taskset");
    ok($c->add_hook($h), "add_hook($h)");
    is($c->{hooks}->{$h}, undef, "no hook $h");
};

subtest "do tast", sub {
    $ENV{AUTHOR_TESTING} || plan skip_all => 'without $ENV{AUTHOR_TESTING}';
    $ENV{GEARMAN_SERVERS}
        || plan skip_all => 'without $ENV{GEARMAN_SERVERS}';

    my $starttime = [Time::HiRes::gettimeofday];

    pass("do_task($tn, $args, {timeout => $timeout})");
    $c->do_task($tn, $args, { timeout => $timeout });

    is(int(Time::HiRes::tv_interval($starttime)), $timeout, "do_task timeout");
};

subtest "_get_random_js_sock", sub {
    if (@{ $c->job_servers() }) {
        ok(my @r = $c->_get_random_js_sock());
        note explain @r;
    }
    else {
        is($c->_get_random_js_sock(), undef);
    }
};

subtest "dispatch background", sub {
    $ENV{AUTHOR_TESTING} || plan skip_all => 'without $ENV{AUTHOR_TESTING}';
    $ENV{GEARMAN_SERVERS}
        || plan skip_all => 'without $ENV{GEARMAN_SERVERS}';

    ok(my $h = $c->dispatch_background($tn, $args),
        "dispatch_background($tn, $args)");
    $h
        && ok($r = $c->get_status($h), "get_status($h)")
        && isa_ok($r, "Gearman::JobStatus");
};

done_testing();
