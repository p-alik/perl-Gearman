use strict;
use warnings;

use IO::Socket::INET;
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

my @js = $ENV{GEARMAN_SERVERS} ? split /,/, $ENV{GEARMAN_SERVERS} : ();
my $c  = new_ok("Gearman::Client");
my $ts = new_ok($mn, [$c]);

is($ts->{cancelled},        0);
is(ref($ts->{hooks}),       "HASH");
is(ref($ts->{loaned_sock}), "HASH");
is(ref($ts->{need_handle}), "ARRAY");
is(ref($ts->{waiting}),     "HASH");
is($ts->client, $c, "client");

throws_ok { $mn->new('a') }
qr/^provided client argument is not a Gearman::Client reference/,
    "caught die off on client argument check";

subtest "hook", sub {
    my $cb = sub { 2 * shift };
    my $h = "ahook";
    ok($ts->add_hook($h, $cb));
    is($ts->{hooks}->{$h}, $cb);
    $ts->run_hook($h, 2);
    ok($ts->add_hook($h));
    is($ts->{hooks}->{$h}, undef);
};

subtest "cancel", sub {
    is($ts->{cancelled}, 0);

    # just in order to test close in cancel sub
    $ts->{default_sock} = IO::Socket::INET->new();
    $ts->{loaned_sock}->{x} = IO::Socket::INET->new();

    $ts->cancel();

    is($ts->{cancelled},          1);
    is($ts->{default_sock},       undef);
    is(keys(%{ $ts->{waiting} }), 0);
    is(@{ $ts->{need_handle} },   0);
    is($ts->{client},             undef);

    delete $ts->{loaned_sock}->{x};
};

subtest "socket", sub {
    $ts->{client} = new_ok("Gearman::Client");
    is($ts->_get_hashed_sock(0), undef);

    $ts->{client} = new_ok("Gearman::Client", [job_servers => [@js]]);
    my @js = @{ $ts->{client}->job_servers() };
    for (my $i = 0; $i < scalar(@js); $i++) {
        ok(my $ls = $ts->_get_loaned_sock($js[$i]),
            "_get_loaned_sock($js[$i])");
        isa_ok($ls, "IO::Socket::INET");
        is($ts->_get_hashed_sock($i),
            $ls, "_get_hashed_sock($i) = _get_loaned_sock($js[$i])");
    } ## end for (my $i = 0; $i < scalar...)

    # random
    ok($ts->_get_default_sock(), "_get_default_sock");
};

# _wait_for_packet
# _is_port
# _fail_jshandle
# _process_packet

subtest "task", sub {

    pass("TODO");

    # is($ts->add_task(qw/a b/), undef, "add_task returns undef");

};

done_testing();
