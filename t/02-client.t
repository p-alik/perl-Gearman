use strict;
use warnings;
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

done_testing();
