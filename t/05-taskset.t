use strict;
use warnings;
use Test::More;
use Test::Exception;

my $mn = "Gearman::Taskset";
use_ok($mn);
use_ok("Gearman::Client");

can_ok(
    "Gearman::Taskset", qw/
        add_task
        add_hook
        run_hook
        cancel
        client
        wait
        _get_loaned_sock
        _get_default_sock
        _get_hashed_sock
        _wait_for_packet
        _ip_port
        _fail_jshandle
        _process_packet
        /
);

my $c = new_ok("Gearman::Client");
my $ts = new_ok($mn, [$c]);
is($ts->client, $c, "client");

is($ts->add_task(qw/a b/), undef, "add_task return undef because no socket");

throws_ok { $mn->new('a') }
qr/^provided client argument is not a Gearman::Client reference/,
    "caught die off on client argument check";

done_testing();
