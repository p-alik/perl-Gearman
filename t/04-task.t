use strict;
use warnings;
use Test::More;

use_ok('Gearman::Task');

my $c = new_ok('Gearman::Task', ['foo', \'bar', { timeout => 0 }]);
is($c->timeout, 0, 'timeout');

is($c->{background}, undef,        '!background');
is($c->mode,         'submit_job', 'submit_job');
is($c->{high_priority} = 1, 1, 'high_priority');
is($c->mode, 'submit_job_high', 'submit_job');

is($c->{background} = 1, 1, 'background');
is($c->mode, 'submit_job_high_bg', 'submit_job_high_bg');
is($c->{high_priority} = 0, 0, '!high_priority');
is($c->mode, 'submit_job_bg', 'submit_job_bg');

done_testing();
