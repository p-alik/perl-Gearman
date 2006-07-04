#!/usr/bin/perl

use strict;
our $Bin;
use FindBin qw( $Bin );
use Gearman::Client;
use Storable qw( freeze );
use Test::More;
use IO::Socket::INET;
use POSIX qw( :sys_wait_h );
use List::Util qw(first);;

use constant PORT => 9000;
our %Children;

END { kill_children() }

if (start_server(PORT)) {
    plan tests => 20;
} else {
    plan skip_all => "Can't find server to test with";
    exit 0;
}

start_server(PORT + 1);

## Sleep, wait for servers to start up before connecting workers.
wait_for_port(PORT);
wait_for_port(PORT + 1);

## Look for 2 job servers, starting at port number PORT.
start_worker(PORT, 2);
start_worker(PORT, 2);

my $client = Gearman::Client->new;
isa_ok($client, 'Gearman::Client');
$client->job_servers('127.0.0.1:' . PORT, '127.0.0.1:' . (PORT + 1));

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
my @worker_pids = grep $Children{$_} eq 'W', keys %Children;
kill INT => @worker_pids[1..$#worker_pids];
$tasks = $client->new_task_set;
$out = '';
$tasks->add_task(echo_ws => 1, { on_complete => sub { $out .= ${ $_[0] } } });
$tasks->add_task(echo_ws => 2, { on_complete => sub { $out .= ${ $_[0] } } });
$tasks->add_task(echo_ws => 'x', {
    on_fail => sub {
        $tasks->add_task(echo_ws => 'p', {
            on_complete => sub { $out .= ${ $_[0] } },
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
ok($handle, 'Got a handle back from dispatching background job');
my $status = $client->get_status($handle);
isa_ok($status, 'Gearman::JobStatus');
ok($status->running, 'Job is still running');
is($status->percent, .5, 'Job is 50 percent complete');
do {
    sleep 1;
    $status = $client->get_status($handle);
} until $status->percent == 1;



sub pid_is_dead {
    my($pid) = @_;
    return if $pid == -1;
    my $type = delete $Children{$pid};
    if ($type eq 'W') {
        ## Right now we can only restart workers.
        start_worker(PORT, 2);
    }
}

sub respawn_children {
    for my $pid (keys %Children) {
        if (waitpid($pid, WNOHANG) > 0) {
            pid_is_dead($pid);
        }
    }
}

sub start_server {
    my($port) = @_;
    my @loc = ("$Bin/../../../../server/gearmand",  # using svn
               '/usr/bin/gearmand',            # where some distros might put it
               '/usr/sbin/gearmand',           # where other distros might put it
               );
    my $server = first { -e $_ } @loc
        or return 0;

    my $pid = start_child([ $server, '-p', $port ]);
    $Children{$pid} = 'S';
    return 1;
}

sub start_worker {
    my($port, $num) = @_;
    my $worker = "$Bin/worker.pl";
    my $servers = join ',',
                  map '127.0.0.1:' . (PORT + $_),
                  0..$num-1;
    my $pid = start_child([ $worker, '-s', $servers ]);
    $Children{$pid} = 'W';
}

sub start_child {
    my($cmd) = @_;
    my $pid = fork();
    die $! unless defined $pid;
    unless ($pid) {
        exec 'perl', '-Iblib/lib', '-Ilib', @$cmd or die $!;
    }
    $pid;
}

sub kill_children {
    kill INT => keys %Children;
}

sub wait_for_port {
    my($port) = @_;
    my $start = time;
    while (1) {
        my $sock = IO::Socket::INET->new(PeerAddr => "127.0.0.1:$port");
        return 1 if $sock;
        select undef, undef, undef, 0.25;
        die "Timeout waiting for port $port to startup" if time > $start + 5;
    }
}
