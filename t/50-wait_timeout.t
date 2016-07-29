use strict;
use warnings;

# OK gearmand v1.0.6
# OK Gearman::Server

use FindBin qw/$Bin/;
use Gearman::Client;
use Test::More;
use Test::Timer;

use lib "$Bin/lib";
use Test::Gearman;

my $tg = Test::Gearman->new(
    ip     => "127.0.0.1",
    daemon => $ENV{GEARMAND_PATH} || undef
);

$tg->start_servers() || plan skip_all => "Can't find server to test with";

($tg->check_server_connection(@{ $tg->job_servers }[0]))
    || plan skip_all => "connection check $_ failed";

plan tests => 3;

$tg->start_worker();

my $client = new_ok("Gearman::Client", [job_servers => $tg->job_servers()]);

subtest "wait with timeout", sub {
    ok(my $tasks = $client->new_task_set, "new_task_set");
    isa_ok($tasks, 'Gearman::Taskset');

    my ($iter, $completed, $failed) = (0, 0, 0);

    my $opt = {
        uniq        => $iter,
        on_complete => sub {
            $completed++;
            note "Got result for $iter";
        },
        on_fail => sub {
            $failed++;
        },
    };

    # For a total of 5 events, that will be 20 seconds; till they complete.
    foreach $iter (1 .. 5) {
        ok($tasks->add_task("long", $iter, $opt), "add_task('long', $iter)");
    }

    my $to = 11;

    time_ok(sub { $tasks->wait(timeout => $to) }, $to, "timeout");
    ok($completed > 0, "at least one job is completed");
    is($failed, 0, "no failed jobs");
};

subtest "long args", sub {
    my $tasks = $client->new_task_set;
    isa_ok($tasks, 'Gearman::Taskset');

    my $arg = 'x' x (5 * 1024 * 1024);

    $tasks->add_task(
        "long",
        \$arg,
        {
            on_complete => sub {
                my $rr = shift;
                if (length($$rr) != length($arg)) {
                    fail(     "Large job failed size check: got "
                            . length($$rr)
                            . ", want "
                            . length($arg));
                } ## end if (length($$rr) != length...)
                elsif ($$rr ne $arg) {
                    fail("Large job failed content check");
                }
                else {
                    pass("Large job succeeded");
                }
            },
            on_fail => sub {
                fail("Large job failed");
            },
        }
    );

    my $to = 10;
    time_ok(sub { $tasks->wait(timeout => $to) }, $to, "timeout");
};
