#!/usr/bin/perl

use strict;
use Gearman::Client;
use Storable qw( freeze );
use Test::More;
use lib 't';
use TestGearman;

if (start_server(PORT)) {
    plan tests => 33;
} else {
    plan skip_all => "Can't find server to test with";
    exit 0;
}

$NUM_SERVERS = 3;

for (1..($NUM_SERVERS-1)) {
    start_server(PORT + $_)
}

# kinda useless, now that start_server does this for us, but...
for (0..($NUM_SERVERS-1)) {
    ## Sleep, wait for servers to start up before connecting workers.
    wait_for_port(PORT + $_);
}

## Start two workers, look for $NUM_SERVERS job servers, starting at
## port number PORT.
start_worker(PORT, $NUM_SERVERS);
start_worker(PORT, $NUM_SERVERS);

my $client = Gearman::Client->new(exceptions => 1);
isa_ok($client, 'Gearman::Client');
$client->job_servers(map { '127.0.0.1:' . (PORT + $_) } 0..$NUM_SERVERS);

eval { $client->do_task(sum => []) };
like($@, qr/scalar or scalarref/, 'do_task does not accept arrayref argument');

my $out = $client->do_task(sum => freeze([ 3, 5 ]));
is($$out, 8, 'do_task returned 8 for sum');

my $tasks = $client->new_task_set;
isa_ok($tasks, 'Gearman::Taskset');
my $sum;
my $failed = 0;
my $completed = 0;
my $handle = $tasks->add_task(sum => freeze([ 3, 5 ]), {
    on_complete => sub { $sum = ${ $_[0] } },
    on_fail => sub { $failed = 1 }
});
$tasks->wait;
is($sum, 8, 'add_task/wait returned 8 for sum');
is($failed, 0, 'on_fail not called on a successful result');

## Now try a task set with 2 tasks, and make sure they are both completed.
$tasks = $client->new_task_set;
my @sums;
$tasks->add_task(sum => freeze([ 1, 1 ]), {
    on_complete => sub { $sums[0] = ${ $_[0] } },
});
$tasks->add_task(sum => freeze([ 2, 2 ]), {
    on_complete => sub { $sums[1] = ${ $_[0] } },
});
$tasks->wait;
is($sums[0], 2, 'First task completed (sum is 2)');
is($sums[1], 4, 'Second task completed (sum is 4)');

## Test some failure conditions:
## Normal failure (worker returns undef or dies within eval).
is($client->do_task('fail'), undef, 'Job that failed naturally returned undef');

## the die message is available in the on_fail sub
my $msg = undef;
$tasks = $client->new_task_set;
$tasks->add_task('fail_die', undef, {
        on_exception => sub { $msg = shift },
});
$tasks->wait;
like($msg, qr/test reason/, 'the die message is available in the on_fail sub');

## Worker process exits.
is($client->do_task('fail_exit'), undef,
    'Job that failed via exit returned undef');
pid_is_dead(wait());

## Worker process times out (takes longer than timeout seconds).
TODO: {
    todo_skip 'timeout is not yet implemented', 1;
    is($client->do_task('sleep', 5, { timeout => 3 }), undef,
        'Job that timed out after 3 seconds returns failure');
}

# Test sleeping less than the timeout
is(${$client->do_task('sleep_three', '1:less')}, 'less',
   'We took less time than the worker timeout');

# Do it three more times to check that 'uniq' (implied '-')
# works okay. 3 more because we need to go past the timeout.
is(${$client->do_task('sleep_three', '1:one')}, 'one',
   'We took less time than the worker timeout, again');

is(${$client->do_task('sleep_three', '1:two')}, 'two',
   'We took less time than the worker timeout, again');

is(${$client->do_task('sleep_three', '1:three')}, 'three',
   'We took less time than the worker timeout, again');

# Now test if we sleep longer than the timeout
is($client->do_task('sleep_three', 5), undef,
   'We took more time than the worker timeout');

# This task and the next one would be hashed with uniq onto the
# previous task, except it failed, so make sure it doesn't happen.
is($client->do_task('sleep_three', 5), undef,
   'We took more time than the worker timeout, again');

is($client->do_task('sleep_three', 5), undef,
   'We took more time than the worker timeout, again, again');

# Check hashing on success, first job sends in 'a' for argument, second job
# should complete and return 'a' to the callback.
{
    my $tasks = $client->new_task_set;
    $tasks->add_task('sleep_three', '2:a', {
        uniq => 'something',
        on_complete => sub { is(${$_[0]}, 'a', "'a' received") },
        on_fail => sub { fail() },
        });

    sleep 1;

    $tasks->add_task('sleep_three', '2:b', {
        uniq => 'something',
        on_complete => sub { is(${$_[0]}, 'a', "'a' received, we were hashed properly") },
        on_fail => sub { fail() },
        });

    $tasks->wait;

}

# Check to make sure there are no hashing glitches with an explicit
# 'uniq' field. Both should fail.
{
    my $tasks = $client->new_task_set;
    $tasks->add_task('sleep_three', '10:a', {
        uniq => 'something',
        on_complete => sub { fail("This can't happen!") },
        on_fail => sub { pass("We failed properly!") },
        });

    sleep 5;

    $tasks->add_task('sleep_three', '10:b', {
        uniq => 'something',
        on_complete => sub { fail("This can't happen!") },
        on_fail => sub { pass("We failed properly again!") },
        });

    $tasks->wait;

}

## Test retry_count.
my $retried = 0;
is($client->do_task('fail' => '', {
    on_retry => sub { $retried++ },
    retry_count => 3,
}), undef, 'Failure response is still failure, even after retrying');
is($retried, 3, 'Retried 3 times');

$tasks = $client->new_task_set;
$completed = 0;
$failed = 0;
$tasks->add_task(fail => '', {
    on_complete => sub { $completed = 1 },
    on_fail => sub { $failed = 1 },
});
$tasks->wait;
is($completed, 0, 'on_complete not called on failed result');
is($failed, 1, 'on_fail called on failed result');

## Test high_priority.
## Create a taskset with 4 tasks, and have the 3rd fail.
## In on_fail, add a new task with high priority set, and make sure it
## gets executed before task 4. To make this reliable, we need to first
## kill off all but one of the worker processes.
my @worker_pids = grep { $Children{$_} eq 'W' } keys %Children;
kill INT => @worker_pids[1..$#worker_pids];
$tasks = $client->new_task_set;
$out = '';
$tasks->add_task(echo_ws => 1, { on_complete => sub { $out .= ${ $_[0] } } });
$tasks->add_task(echo_ws => 2, { on_complete => sub { $out .= ${ $_[0] } } });
$tasks->add_task(echo_ws => 'x', {
    on_fail => sub {
        $tasks->add_task(echo_ws => 'p', {
            on_complete => sub {
                $out .= ${ $_[0] };
            },
            high_priority => 1
        });
    },
});
$tasks->add_task(echo_ws => 3, { on_complete => sub { $out .= ${ $_[0] } } });
$tasks->add_task(echo_ws => 4, { on_complete => sub { $out .= ${ $_[0] } } });
$tasks->add_task(echo_ws => 5, { on_complete => sub { $out .= ${ $_[0] } } });
$tasks->add_task(echo_ws => 6, { on_complete => sub { $out .= ${ $_[0] } } });
$tasks->wait;
like($out, qr/p.+6/, 'High priority tasks executed in priority order.');
## We just killed off all but one worker--make sure they get respawned.
respawn_children();

## Test dispatch_background and get_status.
$handle = $client->dispatch_background(long => undef, {
    on_complete => sub { $out = ${ $_[0] } },
});

# wait for job to start being processed:
sleep 1;

ok($handle, 'Got a handle back from dispatching background job');
my $status = $client->get_status($handle);
isa_ok($status, 'Gearman::JobStatus');
ok($status->known, 'Job is known');
ok($status->running, 'Job is still running');
is($status->percent, .5, 'Job is 50 percent complete');

do {
    sleep 1;
    $status = $client->get_status($handle);
} until $status->percent == 1;

