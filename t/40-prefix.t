#!/usr/bin/perl

use strict;
use Gearman::Client;
use Storable qw( freeze );
use Test::More;
use Time::HiRes 'sleep';

use lib 't';
use TestGearman;



if (start_server(PORT)) {
    plan tests => 9;
} else {
    plan skip_all => "Can't find server to test with";
    exit 0;
}

$NUM_SERVERS = 3;

for (1..($NUM_SERVERS-1)) {
    start_server(PORT + $_)
}

start_worker(PORT, { prefix => 'prefix_a', num_servers => $NUM_SERVERS });
start_worker(PORT, { prefix => 'prefix_b', num_servers => $NUM_SERVERS });

my @job_servers = map { '127.0.0.1:' . (PORT + $_) } 0..$NUM_SERVERS;

my $client_a = Gearman::Client->new(prefix => 'prefix_a');
isa_ok($client_a, 'Gearman::Client');
$client_a->job_servers(@job_servers);

my $client_b = Gearman::Client->new(prefix => 'prefix_b');
isa_ok($client_b, 'Gearman::Client');
$client_b->job_servers(@job_servers);

# basic do_task test 
is(${$client_a->do_task('echo_prefix', 'beep test')}, 'beep test from prefix_a',
   'basic do_task() - prefix a');
is(${$client_b->do_task('echo_prefix', 'beep test')}, 'beep test from prefix_b',
   'basic do_task() - prefix b');

is(${$client_a->do_task(Gearman::Task->new('echo_prefix', \('beep test')))}, 'beep test from prefix_a',
   'Gearman::Task do_task() - prefix a');
is(${$client_b->do_task(Gearman::Task->new('echo_prefix', \('beep test')))}, 'beep test from prefix_b',
   'Gearman::Task do_task() - prefix b');

my %tasks = (
             a => $client_a->new_task_set,
             b => $client_b->new_task_set,
);

my %out; 
for my $k (keys %tasks) {
    $out{$k} = '';
    $tasks{$k}->add_task('echo_prefix' => "$k", { on_complete => sub { $out{$k} .= ${ $_[0] } } });
}
$tasks{$_}->wait for keys %tasks;

for my $k (sort keys %tasks) {
    is($out{$k}, "$k from prefix_$k", "taskset from client_$k");
}

## dispatch_background tasks also support prefixing
my $bg_task = Gearman::Task->new('echo_sleep', \('sleep prefix test'));
my $handle = $client_a->dispatch_background($bg_task);

## wait for the task to be done
my $status;
my $n = 0;
do {
    sleep 0.1;
    $n++;
    diag "still waiting..." if $n == 12;
    $status = $client_a->get_status($handle);
} until $status->percent == 1 or $n == 20;
is $status->percent, 1, "Background task completed using prefix";
