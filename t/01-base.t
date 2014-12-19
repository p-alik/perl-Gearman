use strict;
use warnings;
use Test::More;

use_ok('Gearman::Base');

my @servers = $ENV{GEARMAN_SERVERS}
    ? split /,/, $ENV{GEARMAN_SERVERS}
    : qw/foo bar/;
my $c = new_ok(
    'Gearman::Base',
    [job_servers => $servers[0]],
    "Gearman::Base->new(job_servers => $servers[0])"
);
is(
    @{ $c->job_servers() }[0],
    @{ $c->canonicalize_job_servers($servers[0]) }[0],
    "job_servers=$servers[0]"
);
is(1, $c->{js_count}, 'js_count=1');

$c = new_ok(
    'Gearman::Base',
    [job_servers => [@servers]],
    sprintf("Gearman::Base->new(job_servers => [%s])", join(', ', @servers))
);
is(scalar(@servers), $c->{js_count}, 'js_count=' . scalar(@servers));
ok(my @js = $c->job_servers);
for (my $i = 0; $i <= $#servers; $i++) {
    is(@{ $c->canonicalize_job_servers($servers[$i]) }[0],
        $js[$i], "canonicalize_job_servers($servers[$i])");
}

is($c->debug(),       0,     'debug()');
is($c->debug(1),      1,     'debug(1)');
is($c->prefix(),      undef, 'prefix');
is($c->prefix('foo'), 'foo', 'prefix(foo)');

ok($c->job_servers($servers[0]), "job_servers($servers[0])");
is(
    @{ $c->job_servers() }[0],
    @{ $c->canonicalize_job_servers($servers[0]) }[0],
    'job_servers'
);

ok($c->job_servers([$servers[0]]), "job_servers([$servers[0]])");
is(
    @{ $c->job_servers() }[0],
    @{ $c->canonicalize_job_servers($servers[0]) }[0],
    'job_servers'
);

done_testing();
