use strict;
use warnings;

use FindBin qw/ $Bin /;
use IO::Socket::INET;
use Test::More;
use Test::Exception;

use lib "$Bin/lib";
use Test::Gearman;

my $tg = Test::Gearman->new(
    count  => 3,
    ip     => "127.0.0.1",
    daemon => $ENV{GEARMAND_PATH} || undef
);

my @js = $tg->start_servers() ? @{ $tg->job_servers } : ();
my $mn = "Gearman::Taskset";
use_ok($mn);
use_ok("Gearman::Client");

can_ok(
    $mn, qw/
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

my $c = new_ok("Gearman::Client", [job_servers => [@js]]);
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

    if (scalar(@js)) {
        ok($ts->_get_default_sock(), "_get_default_sock");
        ok($ts->_ip_port($ts->_get_default_sock()));
    }
    else {
        # undef
        is($ts->_get_default_sock(), undef, "_get_default_sock");
        is($ts->_ip_port($ts->_get_default_sock()), undef);
    }

};

subtest "task", sub {
    throws_ok { $ts->_fail_jshandle() } qr/called without shandle/,
        "caught _fail_jshandle() without shandle";

    throws_ok { $ts->_fail_jshandle('x') } qr/unknown handle/,
        "caught _fail_jshandle() unknown shandle";

    dies_ok { $ts->_wait_for_packet() } "_wait_for_packet() dies";
    dies_ok { $ts->add_task() } "add_task() dies";
    my $f = "foo";
    $ts->{need_handle} = [];
    $ts->{client} = new_ok("Gearman::Client", [job_servers => [@js]]);
    if (!@js) {
        is($ts->add_task($f), undef, "add_task($f) returns undef");
    }
    else {
        ok($ts->add_task($f), "add_task($f) returns handle");
        is(scalar(@{ $ts->{need_handle} }), 0);

        #TODO timeout test
        is($ts->_wait_for_packet($ts->_get_default_sock(), 1),
            0, "_wait_for_packet");
    } ## end else [ if (!@js) ]

    # is($ts->add_task(qw/a b/), undef, "add_task returns undef");

};

subtest "_process_packet", sub {
    my $f = "foo";
    my $h = "H:localhost:12345";

    $ts->{need_handle} = [];
    $ts->{client} = new_ok("Gearman::Client", [job_servers => [@js]]);
    my $r = { type => "job_created", blobref => \$h };
    throws_ok { $ts->_process_packet($r, $ts->_get_default_sock()) }
    qr/unexpected job_created/, "job_created exception";

    $ts->{need_handle} = [$ts->client()->_get_task_from_args($f)];
    dies_ok { $ts->_process_packet($r, $ts->_get_default_sock()) }
    "_process_packet dies";
    $r->{type} = "work_fail";
    throws_ok { $ts->_process_packet($r, $ts->_get_default_sock()) }
    qr/work_fail for unknown handle/,
        "caught _process_packet({type => work_fail})";

    $r->{type} = "work_complete";
    throws_ok { $ts->_process_packet($r, $ts->_get_default_sock()) }
    qr/Bogus work_complete from server/,
        "caught _process_packet({type => work_complete})";

    $r->{blobref} = \join "\0", $h, "abc";
    throws_ok { $ts->_process_packet($r, $ts->_get_default_sock()) }
    qr/got work_complete for unknown handle/,
        "caught _process_packet({type => work_complete}) unknown handle";

    $r = { type => "work_exception", blobref => \$h };
    throws_ok { $ts->_process_packet($r, $ts->_get_default_sock()) }
    qr/Bogus work_exception from server/,
        "caught _process_packet({type => work_exception})";
    $r->{blobref} = \join "\0", ${ $r->{blobref} }, "abc";
    throws_ok { $ts->_process_packet($r, $ts->_get_default_sock()) }
    qr/got work_exception for unknown handle/,
        "caught _process_packet({type => work_exception}) unknown handle";

    $r = { type => "work_status", blobref => \$h };
    throws_ok { $ts->_process_packet($r, $ts->_get_default_sock()) }
    qr/got work_status for unknown handle/,
        "caught _process_packet({type => work_status}) unknown handle";

    $r->{type} = $f;
    throws_ok { $ts->_process_packet($r, $ts->_get_default_sock()) }
    qr/unimplemented packet type/,
        "caught _process_packet({type => $f }) unknown handle";
};

done_testing();

