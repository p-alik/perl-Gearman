use strict;
use warnings;

use Gearman::Client;
use Storable qw(thaw);
use Test::More;

use lib 't';
use TestGearman;

my $job_server;
{
    my $port = (free_ports(1))[0];
    if (!start_server($ENV{GEARMAND_PATH}, $port)) {
        plan skip_all => "Can't find server to test with";
        exit 0;
    }

    my $la = "127.0.0.1";
    $job_server = join ':', $la, $port;

    check_server_connection($job_server);
    start_worker([$job_server]);
}

plan tests => 5;

my $client = new_ok("Gearman::Client", [job_servers => [$job_server]]);

subtest "stop if subtest 1", sub {

    # If we start up too fast, then the worker hasn't gone 'idle' yet.
    sleep 1;

    my $result = $client->do_task('check_stop_if');
    my ($is_idle, $last_job_time) = @{ thaw($$result) };

    is($is_idle,       0,     "We shouldn't be idle yet");
    is($last_job_time, undef, "No job should have been processed yet");
};

subtest "stop if subtest 2", sub {
    my $result = $client->do_task('check_stop_if');
    my ($is_idle, $last_job_time) = @{ thaw($$result) };

    is($is_idle, 0, "We still shouldn't be idle yet");
    isnt($last_job_time, undef, "We should have processed a job now");

    my $time_diff = time() - $last_job_time;

    # On a really slow system this test could fail, maybe.
    ok($time_diff < 3,
        "That last job should have been within the last 3 seconds");
};

subtest "stop if subtest 3", sub {
    note "Sleeping for 5 seconds";
    sleep 5;

    my $result = $client->do_task('check_stop_if');
    my ($is_idle, $last_job_time) = @{ thaw($$result) };

    is($is_idle, 0, "We still shouldn't be idle yet");
    isnt($last_job_time, undef, "We should have processed a job now");

    my $time_diff = time() - $last_job_time;

    # On a really slow system this test could fail, maybe.
    ok($time_diff > 3,
        "That last job should have been more than 3 seconds ago");
    ok($time_diff < 8,
        "That last job should have been less than 8 seconds ago");
};

subtest "stop if subtest 4", sub {
    $client->do_task('work_exit');

    # make sure the worker has time to shut down and isn't still in the 'run' loop
    sleep 2;

    my $result = $client->do_task('check_stop_if');
    my ($is_idle, $last_job_time) = @{ thaw($$result) };

    is($is_idle,       0,     "We shouldn't be idle yet");
    is($last_job_time, undef, "No job should have been processed yet");
};

