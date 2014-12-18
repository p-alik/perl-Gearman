use strict;
use warnings;
use Test::More;

unless ($ENV{GEARMAN_SERVERS}) {
    plan skip_all => 'Gearman::Base tests without $ENV{GEARMAN_SERVERS}';
    exit;
}

my @servers = split /,/, $ENV{GEARMAN_SERVERS};

use_ok('Gearman::Base');

my $c = new_ok('Gearman::Base', [job_servers => [@servers]]);

is(scalar(@servers), $c->{js_count}, 'js_count');
is(scalar(@servers), scalar(@{ $c->job_servers() }), 'job_servers');
is(@{ $c->canonicalize_job_servers('foo') }[0],
    'foo:4730', 'canonicalize_job_servers(foo)');
is(@{ $c->canonicalize_job_servers('foo:123') }[0],
    'foo:123', 'canonicalize_job_servers(foo:123)');

is($c->debug(),  0, 'debug()');
is($c->debug(1), 1, 'debug(1)');

done_testing();
