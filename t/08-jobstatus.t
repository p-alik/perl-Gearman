use strict;
use warnings;

use Test::More;


my ($mn) = qw/
    Gearman::JobStatus
    /;

use_ok($mn);


can_ok(
    $mn, qw/
        known
        percent
        progress
        running
        /
);

new_ok($mn, []);


done_testing();

