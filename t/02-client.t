use strict;
use warnings;

use Time::HiRes qw/
    gettimeofday
    tv_interval
    /;

use Test::More;

unless ($ENV{GEARMAN_SERVERS}) {
    plan skip_all => 'Gearman::Client tests without $ENV{GEARMAN_SERVERS}';
    exit;
}

use_ok('Gearman::Client');

can_ok(
    'Gearman::Client', qw/
        _job_server_status_command
        _get_js_sock
        _get_random_js_sock
        _get_task_from_args
        _option_request
        _put_js_sock
        /
);

my $c = new_ok('Gearman::Client',
    [job_servers => [split /,/, $ENV{GEARMAN_SERVERS}]]);
isa_ok($c, 'Gearman::Base');

isa_ok($c->new_task_set(), 'Gearman::Taskset');

ok(my $r = $c->get_job_server_status, 'get_job_server_status');
note 'get_job_server_status result: ', explain $r;

ok($r = $c->get_job_server_jobs, 'get_job_server_jobs');
note 'get_job_server_jobs result: ', explain $r;

ok($r = $c->get_job_server_clients, 'get_job_server_clients');
note 'get_job_server_clients result: ', explain $r;

my $starttime = [Time::HiRes::gettimeofday];
my ($tn, $args, $timeout) = qw/foo bar 2/;
pass("do_task($tn, $args, {timeout => $timeout})");
$c->do_task($tn, $args, { timeout => $timeout });
is(int(Time::HiRes::tv_interval($starttime)), $timeout, 'do_task timeout');

ok(my $h = $c->dispatch_background($tn, $args),
    "dispatch_background($tn, $args)");
$h && ok($r = $c->get_status($h), "get_status($h)");
note 'get_status result: ', explain $r;

done_testing();
