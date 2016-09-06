use strict;
use warnings;

# OK gearmand v1.0.6

use File::Which qw/ which /;
use List::Util qw/ sum /;
use Test::Exception;
use Test::More;

use t::Server qw/ new_server /;
use t::Worker qw/ new_worker /;

use Storable qw/
    freeze
    thaw
    /;

my $daemon = "gearmand";
my $bin    = $ENV{GEARMAND_PATH} || which($daemon);
my $host   = "127.0.0.1";

$bin      || plan skip_all => "Can't find $daemon to test with";
(-X $bin) || plan skip_all => "$bin is not executable";

my %job_servers;

for (0 .. int(rand(1) + 1)) {
    my $gs = new_server($bin, $host);
    $gs || BAIL_OUT "couldn't start $bin";

    $job_servers{ join(':', $host, $gs->port) } = $gs;
} ## end for (0 .. int(rand(1) +...))

use_ok("Gearman::Client");

my $client = new_ok("Gearman::Client",
    [exceptions => 1, job_servers => [keys %job_servers]]);

my $func = "sum";
my $cb   = sub {
    my $sum = 0;
    $sum += $_ for @{ thaw($_[0]->arg) };
    return $sum;
};

my @workers
    = map(
    new_worker(job_servers => [keys %job_servers], func => { $func, $cb }),
    (0 .. int(rand(1) + 1)));

subtest "taskset 1", sub {
    throws_ok { $client->do_task(sum => []) }
    qr/Function argument must be scalar or scalarref/,
        'do_task does not accept arrayref argument';

    my @a   = _rl();
    my $sum = sum(@a);
    my $out = $client->do_task(sum => freeze([@a]));
    is($$out, $sum, "do_task returned $sum for sum");

    undef($out);

    my $tasks = $client->new_task_set;
    isa_ok($tasks, 'Gearman::Taskset');

    my $failed = 0;
    my $handle = $tasks->add_task(
        sum => freeze([@a]),
        {
            on_complete => sub { $out    = ${ $_[0] } },
            on_fail     => sub { $failed = 1 }
        }
    );

    note "wait";
    $tasks->wait;

    is($out,    $sum, "add_task/wait returned $sum for sum");
    is($failed, 0,    'on_fail not called on a successful result');
};

subtest "taskset 2", sub {
    my $ts = $client->new_task_set;

    my @a  = _rl();
    my $sa = sum(@a);
    my @sums;
    $ts->add_task(
        sum => freeze([@a]),
        { on_complete => sub { $sums[0] = ${ $_[0] } }, }
    );
    my @b  = _rl();
    my $sb = sum(@b);
    $ts->add_task(
        sum => freeze([@b]),
        { on_complete => sub { $sums[1] = ${ $_[0] } }, }
    );
    note "wait";
    $ts->wait;

    is($sums[0], $sa, "First task completed (sum is $sa)");
    is($sums[1], $sb, "Second task completed (sum is $sb)");
};

done_testing();

sub _rl {
    return map { int(rand(100)) } (0 .. int(rand(10) + 1));
}
