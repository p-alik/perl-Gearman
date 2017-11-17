use strict;
use warnings;

# OK gearmand v1.0.6

use List::Util qw/ sum /;
use Storable qw/
    freeze
    thaw
    /;
use Test::Exception;
use Test::More;

use lib '.';
use t::Server ();
use t::Worker qw/ new_worker /;

my $gts         = t::Server->new();
my @job_servers = $gts->job_servers(int(rand(1) + 1));
@job_servers || plan skip_all => $t::Server::ERROR;
plan tests => 3;

use_ok("Gearman::Client");

my $client = new_ok("Gearman::Client",
    [exceptions => 1, job_servers => [@job_servers]]);

my $data_size = 200_000;
my $func = "bigdata";
my $cb   = sub {
    return '~' x $data_size;
};

my @workers
    = map(new_worker(job_servers => [@job_servers], func => { $func, $cb }),
    (0 .. 1));

subtest "add_task() call-back adding more tasks via add_task()", sub {
    plan tests => 4;

    my $loops = 10;
    my $re_add_every = 3;

    isa_ok my $ts = $client->new_task_set, "Gearman::Taskset";

    my $failed = 0;
    my $incorrect = 0;
    my $passed = 0;
    my $on_complete_cb = sub { length( ${ $_[0] } ) == $data_size ? $passed++ : $incorrect++; };
    my $on_fail_cb = sub { $failed++ };
    foreach my $i (1..$loops) {
        $ts->add_task(
            bigdata => freeze( [] ), {
               on_complete => sub {
                   $on_complete_cb->(@_);
                   if ($i % $re_add_every == 0) {
                       $ts->add_task(
                                      bigdata => freeze( [] ), {
                                        on_complete => $on_complete_cb,
                                        on_fail     => $on_fail_cb,
                                      }
                       );
                    }
               },
               on_fail => $on_fail_cb,
            }
            ) || $failed++;
    }

    note "wait";
    $ts->wait;

    is($passed, ($loops + int($loops / $re_add_every)), "all tasks passed");
    is($incorrect, 0, "all correct");
    is($failed, 0, "no fails");
};


done_testing();
