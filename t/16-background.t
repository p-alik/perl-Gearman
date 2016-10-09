use strict;
use warnings;

# OK gearmand v1.0.6

use Test::More;
use t::Server ();
use t::Worker qw/ new_worker /;

my $gts = t::Server->new();
$gts || plan skip_all => $t::Server::ERROR;

my $job_server = $gts->job_servers();
$job_server || BAIL_OUT "couldn't start ", $gts->bin();

use_ok("Gearman::Client");

my $client = new_ok("Gearman::Client",
    [exceptions => 1, job_servers => [$job_server]]);

my $func = "long";

my $worker = new_worker(
    job_servers => [$job_server],
    func        => {
        $func => sub {
            my ($job) = @_;
            $job->set_status(50, 100);
            sleep 2;
            $job->set_status(100, 100);
            sleep 2;
            return $job->arg;
            }
    }
);

## Test dispatch_background and get_status.
subtest "dispatch background", sub {
    my $handle = $client->dispatch_background(
        $func => undef,
        { on_complete => sub { note "complete", ${ $_[0] } }, }
    );

    # wait for job to start being processed:
    sleep 1;

    ok($handle, 'Got a handle back from dispatching background job');
    ok(my $status = $client->get_status($handle), "get_status");
    ok($status->known,   'Job is known');
    ok($status->running, 'Job is still running');
    is($status->percent, .5, 'Job is 50 percent complete');

    do {
        sleep 1;
        $status = $client->get_status($handle);
        note $status->percent;
    } until $status->percent == 1;
};

done_testing();

