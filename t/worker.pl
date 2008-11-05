#!/usr/bin/perl -w
use strict;

use lib 'lib';
use Gearman::Worker;
use Storable qw(thaw nfreeze);
use Getopt::Long qw( GetOptions );

GetOptions(
           's|servers=s' => \(my $servers),
           'n=i'         => \(my $notifypid),
           'p=s'         => \(my $prefix),
          );

die "usage: $0 -s <servers>" unless $servers;
my @servers = split /,/, $servers;

my $worker = Gearman::Worker->new($prefix ? (prefix => $prefix) : ());
$worker->job_servers(@servers);

$worker->register_function(sum => sub {
    my $sum = 0;
    $sum += $_ for @{ thaw($_[0]->arg) };
    $sum;
});

$worker->register_function(fail => sub { undef });
$worker->register_function(fail_die => sub { die 'test reason' });
$worker->register_function(fail_exit => sub { exit 255 });

$worker->register_function(sleep => sub { sleep $_[0]->arg });
$worker->register_function(sleep_three => 3 => sub {
    my ($sleep, $return) = $_[0]->arg =~ m/^(\d+)(?::(.+))?$/;
    sleep $sleep;
    return $return;
});

$worker->register_function(echo_ws => sub {
    select undef, undef, undef, 0.25;
    $_[0]->arg eq 'x' ? undef : $_[0]->arg;
});

$worker->register_function(echo_prefix => sub {
    join " from ", $_[0]->arg, $prefix;
});

$worker->register_function(echo_sleep => sub {
    my($job) = @_;
    $job->set_status(1, 1);
    sleep 2; ## allow some time to read the status
    join " from ", $_[0]->arg, $prefix;
});


$worker->register_function(long => sub {
    my($job) = @_;
    $job->set_status(50, 100);
    sleep 2;
    $job->set_status(100, 100);
    sleep 2;
});

my $nsig;
$nsig = kill 'USR1', $notifypid if $notifypid;

my $work_exit = 0;

$worker->register_function(work_exit => sub {
    $work_exit = 1;
});

my ($is_idle, $last_job_time);

$worker->register_function(check_stop_if => sub {
    return nfreeze([$is_idle, $last_job_time]);
});



my $stop_if = sub {
    ($is_idle, $last_job_time) = @_;

    if ($work_exit) {
        $work_exit = 0;
        return 1;
    }

    return 0;
};

$worker->work(stop_if => $stop_if) while (1);
