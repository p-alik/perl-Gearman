use strict;
use warnings;

use Time::HiRes qw/
    gettimeofday
    tv_interval
    /;

use Test::More;

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

# note "get_job_server_status result: ", explain $r;

ok($r = $c->get_job_server_jobs, "get_job_server_jobs");
note "get_job_server_jobs result: ", explain $r;

ok($r = $c->get_job_server_clients, "get_job_server_clients");
note "get_job_server_clients result: ", explain $r;

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

subtest "dispatch background", sub {
    $ENV{AUTHOR_TESTING} || plan skip_all => 'without $ENV{AUTHOR_TESTING}';
    $ENV{GEARMAN_SERVERS}
        || plan skip_all => 'without $ENV{GEARMAN_SERVERS}';

    ok(my $h = $c->dispatch_background($tn, $args),
        "dispatch_background($tn, $args)");
    $h && ok($r = $c->get_status($h), "get_status($h)");
    note "get_status result: ", explain $r;
};

done_testing();
