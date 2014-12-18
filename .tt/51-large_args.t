#!/usr/bin/perl

use strict;
use Gearman::Client;
use Storable qw( freeze );
use Test::More;
use Time::HiRes qw(time);

use lib 't';
use TestGearman;

# This is testing the MAXQUEUE feature of gearmand. There's no direct
# support for it in Gearman::Worker yet, so we connect directly to
# gearmand to configure it for the test.

if (start_server(PORT)) {
    plan tests => 3;
} else {
    plan skip_all => "Can't find server to test with";
    exit 0;
}

wait_for_port(PORT);

start_worker(PORT);

my $client = Gearman::Client->new;
isa_ok($client, 'Gearman::Client');

$client->job_servers('127.0.0.1:' . PORT);

my $tasks = $client->new_task_set;
isa_ok($tasks, 'Gearman::Taskset');

my $arg = "x" x ( 5 * 1024 * 1024 );

$tasks->add_task('long', \$arg, {
    on_complete => sub {
        my $rr = shift;
        if (length($$rr) != length($arg)) {
            fail("Large job failed size check: got ".length($$rr).", want ".length($arg));
        } elsif ($$rr ne $arg) {
            fail("Large job failed content check");
        } else {
            pass("Large job succeeded");
        }
    },
    on_fail     => sub {
        fail("Large job failed");
    },
});

$tasks->wait(timeout => 10);

# vim: filetype=perl
