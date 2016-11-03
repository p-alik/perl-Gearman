use strict;
use warnings;

# OK gearmand v1.0.6
use Proc::Guard;
use Test::Exception;
use Test::More;

use Storable qw/
    freeze
    thaw
    /;

use t::Server ();

my $gts = t::Server->new();
$gts || plan skip_all => $t::Server::ERROR;

use_ok("Gearman::Client");
use_ok("Gearman::Worker");

my @job_servers = $gts->job_servers(int(rand(1) + 1));
my $func        = "sum";
my $worker      = worker(job_servers => [@job_servers]);

my @a = map { int(rand(100)) } (0 .. int(rand(10) + 5));
subtest "work_data", sub {
    plan tests => scalar(@a) + 1;

    my $client = new_ok("Gearman::Client", [job_servers => [@job_servers]]);

    my ($i, $r) = (0, 0);
    $client->do_task(
        $func => freeze([@a]),
        {
            on_data => sub {
                my ($ref) = @_;
                $r += $a[$i];
                is($r, ${$ref}, "sub $func");
                $i++;
            }
        }
    );
};

done_testing();

sub worker {
    my (%args) = @_;
    my $w = Gearman::Worker->new(%args);

    my $cb = sub {
        my ($job) = @_;
        my $sum   = 0;
        my @i     = @{ thaw($job->arg) };
        foreach (@i) {
            $sum += $_;
            $w->send_work_data($job, $sum);
        }
        return $sum;
    };

    $w->register_function($func, $cb);

    my $pg = Proc::Guard->new(
        code => sub {
            $w->work(
                stop_if => sub {
                    my ($idle) = @_;
                    return $idle;
                }
            );
        }
    );

    return $pg;
} ## end sub worker
