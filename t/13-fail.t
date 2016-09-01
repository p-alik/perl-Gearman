use strict;
use warnings;

# OK gearmand v1.0.6

use File::Which qw/ which /;
use Test::More;
use t::Server qw/ new_server /;
use t::Worker qw/ new_worker /;

my $daemon = "gearmand";
my $bin    = $ENV{GEARMAND_PATH} || File::Which::which($daemon);
my $host   = "127.0.0.1";

$bin      || plan skip_all => "Can't find $daemon to test with";
(-X $bin) || plan skip_all => "$bin is not executable";

my %job_servers;

for (0 .. int(rand(2) + 1)) {
    my $gs = new_server($bin, $host);
    $gs || BAIL_OUT "couldn't start $bin";

    $job_servers{ join(':', $host, $gs->port) } = $gs;
} ## end for (0 .. int(rand(2) +...))

use_ok("Gearman::Client");

my $client = new_ok("Gearman::Client",
    [exceptions => 1, job_servers => [keys %job_servers]]);

## Test some failure conditions:
## Normal failure (worker returns undef or dies within eval).
subtest "failures", sub {
    my %cb = (
        fail     => sub {undef},
        fail_die => sub { die "test reason" },
    );

    my @workers
        = map(new_worker([keys %job_servers], %cb), (0 .. int(rand(1) + 1)));
    is($client->do_task("fail"),
        undef, "Job that failed naturally returned undef");

    # the die message is available in the on_fail sub
    my $msg   = undef;
    my $tasks = $client->new_task_set;
    $tasks->add_task("fail_die", undef,
        { on_exception => sub { $msg = shift }, });
    $tasks->wait;
    like(
        $msg,
        qr/test reason/,
        "the die message is available in the on_fail sub"
    );

    $tasks = $client->new_task_set;
    my ($completed, $failed) = (0, 0);
    $tasks->add_task(
        fail => '',
        {
            on_complete => sub { $completed = 1 },
            on_fail     => sub { $failed    = 1 },
        }
    );
    $tasks->wait;
    is($completed, 0, 'on_complete not called on failed result');
    is($failed,    1, 'on_fail called on failed result');

    ## Test retry_count.
    my $retried = 0;
    is(
        $client->do_task(
            "fail" => '',
            {
                on_retry    => sub { $retried++ },
                retry_count => 3,
            }
        ),
        undef,
        "Failure response is still failure, even after retrying"
    );
    is($retried, 3, "Retried 3 times");
};

## Worker process exits.
subtest "worker process exits", sub {
    plan skip_all => "TODO supported only by Gearman::Server";

    my @workers
        = map(new_worker([keys %job_servers], fail_exit => sub { exit 255 }),
        (0 .. int(rand(1) + 1)));
    is(
        $client->do_task(
            "fail_exit",
            undef,
            {
                on_fail     => sub { warn "on fail" },
                on_complete => sub { warn "on success" },
                on_status   => sub { warn "on status" }
            }
        ),
        undef,
        "Job that failed via exit returned undef"
    );
};

done_testing();

