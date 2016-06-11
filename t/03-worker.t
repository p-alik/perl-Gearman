use strict;
use warnings;
use Test::More;

my @js = $ENV{GEARMAN_SERVERS} ? split /,/, $ENV{GEARMAN_SERVERS} : ();

my $mn = "Gearman::Worker";

use_ok($mn);

my $c = new_ok($mn, [job_servers => [@js]]);
isa_ok($c, 'Gearman::Objects');

can_ok($mn, qw/
  reset_abilities
  register_function
  unregister_function
  uncache_sock
  work

  /);
my ($tn) = qw/foo/;
ok(
    $c->register_function(
        $tn => sub {
            my ($j) = @_;
            note join(' ', 'work on', $j->handle, explain $j->arg);
            return $j->arg ? $j->arg : 'done';
        }
    ),
    "register_function($tn)"
);

subtest "work", sub {
    $ENV{AUTHOR_TESTING} || plan skip_all => 'without $ENV{AUTHOR_TESTING}';
    $ENV{GEARMAN_SERVERS}
        || plan skip_all => 'without $ENV{GEARMAN_SERVERS}';

    pass "work subtest";
    $c->work(stop_if => sub { return 1; });
};

done_testing();
