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

my @a = map { int(rand(100)) } (0 .. int(rand(10) + 5));
subtest "work_data", sub {
    plan tests => 3;

    my $client = new_ok("Gearman::Client", [job_servers => [@job_servers]]);
    my $worker = worker("send_work_data", job_servers => [@job_servers]);

    my ($i, $r) = (0, 0);
    my $res = $client->do_task(
        $func => freeze([@a]),
        {
            on_data => sub {
                my ($ref) = @_;
                $r += $a[$i];
                $i++;
            }
        }
    );
    is(scalar(@a), $i);
    is(${$res},    $r);
};

subtest "work_warning", sub {
    plan tests => 3;

    my $client = new_ok("Gearman::Client", [job_servers => [@job_servers]]);
    my $worker = worker("send_work_warning", job_servers => [@job_servers]);

    my ($i, $r) = (0, 0);
    my $res = $client->do_task(
        $func => freeze([@a]),
        {
            on_warning => sub {
                my ($ref) = @_;
                $r += $a[$i];
                $i++;
            }
        }
    );
    is(scalar(@a), $i);
    is(${$res},    $r);
};

done_testing();

sub worker {
    my ($send_method, %args) = @_;
    my $w = Gearman::Worker->new(%args);

    my $cb = sub {
        my ($job) = @_;
        my $sum   = 0;
        my @i     = @{ thaw($job->arg) };
        foreach (@i) {
            $sum += $_;
            $w->$send_method($job, $sum);
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
