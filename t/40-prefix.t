use strict;
use warnings;

# OK gearmand v1.0.6
# NOK Gearman::Server
#
use FindBin qw/$Bin/;
use Gearman::Client;
use Storable qw/freeze/;
use Test::More;
use Time::HiRes qw/sleep/;

use lib "$Bin/lib";
use Test::Gearman;

my $tg = Test::Gearman->new(
    count  => 3,
    ip     => "127.0.0.1",
    daemon => $ENV{GEARMAND_PATH} || undef
);

$tg->start_servers() || plan skip_all => "Can't find server to test with";

foreach (@{ $tg->job_servers }) {
    unless ($tg->check_server_connection($_)) {
        plan skip_all => "connection check $_ failed";
        last;
    }
} ## end foreach (@{ $tg->job_servers...})

plan tests => 5;

my @worker_pids;
foreach (qw/a b/) {
    my $pid = $tg->start_worker({ prefix => join('_', "prefix", $_) });
    $pid || die "coundn't start worker";
    push @worker_pids, $pid;
}

my $client_a = new_ok("Gearman::Client",
    [prefix => "prefix_a", job_servers => $tg->job_servers]);
my $client_b = new_ok("Gearman::Client",
    [prefix => "prefix_b", job_servers => $tg->job_servers]);

# basic do_task test
subtest "basic do task", sub {
    is(
        ${ $client_a->do_task('echo_prefix', 'beep test') },
        'beep test from prefix_a',
        'basic do_task() - prefix a'
    );
    is(
        ${ $client_b->do_task('echo_prefix', 'beep test') },
        'beep test from prefix_b',
        'basic do_task() - prefix b'
    );

    is(
        ${
            $client_a->do_task(
                Gearman::Task->new('echo_prefix', \('beep test'))
            )
        },
        'beep test from prefix_a',
        'Gearman::Task do_task() - prefix a'
    );
    is(
        ${
            $client_b->do_task(
                Gearman::Task->new('echo_prefix', \('beep test'))
            )
        },
        'beep test from prefix_b',
        'Gearman::Task do_task() - prefix b'
    );
};

subtest "echo prefix", sub {
    my %out;
    my %tasks = (
        a => $client_a->new_task_set,
        b => $client_b->new_task_set,
    );

    for my $k (keys %tasks) {
        $out{$k} = '';
        $tasks{$k}->add_task(
            'echo_prefix' => "$k",
            {
                on_complete => sub { $out{$k} .= ${ $_[0] } }
            }
        );
    } ## end for my $k (keys %tasks)

    $tasks{$_}->wait for keys %tasks;

    for my $k (sort keys %tasks) {
        is($out{$k}, "$k from prefix_$k", "taskset from client_$k");
    }
};

## dispatch_background tasks also support prefixing
subtest "dispatch background", sub {
    my $bg_task
        = new_ok("Gearman::Task", ['echo_sleep', \('sleep prefix test')]);
    ok(my $handle = $client_a->dispatch_background($bg_task),
        "dispatch_background returns a handle");

    # wait for the task to be done
    my $status;
    my $n = 0;
    do {
        sleep 0.1;
        $n++;
        diag "still waiting..." if $n == 12;
        $status = $client_a->get_status($handle);
    } until $status->percent == 1 or $n == 20;

    is($status->percent, 1, "Background task completed using prefix");
};

