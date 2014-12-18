use strict;
use warnings;
use Test::More;

unless ($ENV{GEARMAN_SERVERS}) {
    plan skip_all => 'Gearman::Worker tests without $ENV{GEARMAN_SERVERS}';
    exit;
}

my @servers = split /,/, $ENV{GEARMAN_SERVERS};

use_ok('Gearman::Worker');

my $c = new_ok(
    'Gearman::Worker',
    [
        job_servers => [split /,/, $ENV{GEARMAN_SERVERS}],
        debug       => 2,
        prefix      => 'foo'
    ]
);
isa_ok($c, 'Gearman::Base');

is($c->debug,  2,     'debug');
is($c->prefix, 'foo', 'prefix');

done_testing();
