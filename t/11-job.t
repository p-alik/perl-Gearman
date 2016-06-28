use strict;
use warnings;

use Test::More;
use Test::Exception;

my ($mn) = qw/
    Gearman::Job
    /;

use_ok($mn);

can_ok(
    $mn, qw/
        set_status
        argref
        arg
        handle
        /
);

my @arg = qw/
    foo
    2
    123.321.1.1:123
    bar
    /;

$arg[1] = \$arg[1];
my $j = new_ok($mn, [@arg]);

is($j->handle(), $arg[2]);
is($j->argref(), $arg[1]);
is($j->arg(),    ${ $arg[1] });

dies_ok { $j->set_status(qw/a b/) };

done_testing();

