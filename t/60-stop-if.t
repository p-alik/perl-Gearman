#!/usr/bin/perl

use strict;
use Gearman::Client;
use Storable qw(thaw);
use Test::More;

use lib 't';
use TestGearman;

if (start_server(PORT)) {
    plan tests => 12;
} else {
    plan skip_all => "Can't find server to test with";
    exit 0;
}

wait_for_port(PORT);

start_worker(PORT);

my $client = Gearman::Client->new;
isa_ok($client, 'Gearman::Client');

$client->job_servers('127.0.0.1:' . PORT);

{
    my $result = $client->do_task('check_stop_if');

    my ($is_idle, $last_job_time) = @{thaw($$result)};

    is($is_idle, 0, "We shouldn't be idle yet");
    is($last_job_time, undef, "No job should have been processed yet");
}

{
    my $result = $client->do_task('check_stop_if');

    my ($is_idle, $last_job_time) = @{thaw($$result)};

    is($is_idle, 0, "We still shouldn't be idle yet");
    isnt($last_job_time, undef, "We should have processed a job now");

    my $time_diff = time() - $last_job_time;

    # On a really slow system this test could fail, maybe.
    ok($time_diff < 3, "That last job should have been within the last 3 seconds");
}

diag "Sleeping for 5 seconds";
sleep 5;

{
    my $result = $client->do_task('check_stop_if');

    my ($is_idle, $last_job_time) = @{thaw($$result)};

    is($is_idle, 0, "We still shouldn't be idle yet");
    isnt($last_job_time, undef, "We should have processed a job now");

    my $time_diff = time() - $last_job_time;

    # On a really slow system this test could fail, maybe.
    ok($time_diff > 3, "That last job should have been more than 3 seconds ago");
    ok($time_diff < 8, "That last job should have been less than 8 seconds ago");
}

$client->do_task('work_exit');

sleep 2; # make sure the worker has time to shut down and isn't still in the 'run' loop

{
    my $result = $client->do_task('check_stop_if');

    my ($is_idle, $last_job_time) = @{thaw($$result)};

    is($is_idle, 0, "We shouldn't be idle yet");
    is($last_job_time, undef, "No job should have been processed yet");
}

# vim: filetype=perl
