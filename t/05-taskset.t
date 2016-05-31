use strict;
use warnings;
use Test::More;

use_ok("Gearman::Taskset");
use_ok("Gearman::Client");

can_ok(
    "Gearman::Taskset", qw/
        add_task
        client
        /
);

my $c = new_ok("Gearman::Client");
my $ts = new_ok("Gearman::Taskset", [$c]);
is($ts->client, $c);

#ok($ts->add_task(qw/a b/));

is($ts->client, $c);

done_testing();
