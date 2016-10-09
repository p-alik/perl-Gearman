use strict;
use warnings;

use File::Which ();
use IO::Socket::IP;
use Test::More;
use Test::Exception;
use t::Server ();

my @js;
my ($cn, $mn) = qw/
    Gearman::Client
    Gearman::Taskset
    /;
use_ok($mn);
use_ok($cn);

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
        process_packet
        /
);

my $c = new_ok($cn, [job_servers => [@js]]);
my $ts = new_ok($mn, [$c]);

is($ts->{cancelled},        0,       "cancelled");
is(ref($ts->{hooks}),       "HASH",  "hooks");
is(ref($ts->{loaned_sock}), "HASH",  "loaned_sock");
is(ref($ts->{need_handle}), "ARRAY", "need_handle");
is(ref($ts->{waiting}),     "HASH",  "waiting");
is($ts->client,             $c,      "client");

throws_ok { $mn->new('a') }
qr/^provided client argument is not a $cn reference/,
    "caught die off on client argument check";

subtest "hook", sub {
    my $cb = sub { 2 * shift };
    my $h = "ahook";
    ok($ts->add_hook($h, $cb), "add_hook($h, ..)");
    is($ts->{hooks}->{$h}, $cb, "$h is a cb");
    $ts->run_hook($h, 2, "run_hook($h)");
    ok($ts->add_hook($h), "add_hook($h, undef)");
    is($ts->{hooks}->{$h}, undef, "$h undef");
};

subtest "cancel", sub {
    is($ts->{cancelled}, 0);

    # just in order to test close in cancel sub
    $ts->{default_sock} = IO::Socket::IP->new();
    $ts->{loaned_sock}->{x} = IO::Socket::IP->new();

    $ts->cancel();

    is($ts->{cancelled},          1,     "cancelled");
    is($ts->{default_sock},       undef, "default_sock");
    is(keys(%{ $ts->{waiting} }), 0,     "waiting");
    is(@{ $ts->{need_handle} },   0,     "need_handle");
    is($ts->{client},             undef, "client");

    delete $ts->{loaned_sock}->{x};
};

subtest "socket", sub {
    my $gts = t::Server->new();
    $gts || plan skip_all => $t::Server::ERROR;

    my $job_server = $gts->job_servers();
    $job_server || plan skip_all => "couldn't start ", $gts->bin();

    my $c = new_ok($cn, [job_servers => [$job_server]]);
    my $ts = new_ok($mn, [$c]);

    my @js = @{ $ts->{client}->job_servers() };
    for (my $i = 0; $i < scalar(@js); $i++) {

        ok(my $ls = $ts->_get_loaned_sock($js[$i]),
            "_get_loaned_sock($js[$i])");
        isa_ok($ls, "IO::Socket::IP");
        is($ts->_get_hashed_sock($i),
            $ls, "_get_hashed_sock($i) = _get_loaned_sock($js[$i])");
    } ## end for (my $i = 0; $i < scalar...)

    ok($ts->_get_default_sock(),                "_get_default_sock");
    ok($ts->_ip_port($ts->_get_default_sock()), "_ip_port");
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
    $ts->{client} = new_ok($cn, [job_servers => [@js]]);
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

subtest "process_packet", sub {
    my $f = "foo";
    my $h = "H:localhost:12345";

    $ts->{need_handle} = [];
    $ts->{client} = new_ok("Gearman::Client", [job_servers => [@js]]);
    my $r = { type => "job_created", blobref => \$h };
    throws_ok { $ts->process_packet($r, $ts->_get_default_sock()) }
    qr/unexpected job_created/, "job_created exception";

    $ts->{need_handle} = [$ts->client()->_get_task_from_args($f)];
    dies_ok { $ts->process_packet($r, $ts->_get_default_sock()) }
    "process_packet dies";
    $r->{type} = "work_fail";
    throws_ok { $ts->process_packet($r, $ts->_get_default_sock()) }
    qr/work_fail for unknown handle/,
        "caught process_packet({type => work_fail})";

    $r->{type} = "work_complete";
    throws_ok { $ts->process_packet($r, $ts->_get_default_sock()) }
    qr/Bogus work_complete from server/,
        "caught process_packet({type => work_complete})";

    $r->{blobref} = \join "\0", $h, "abc";
    throws_ok { $ts->process_packet($r, $ts->_get_default_sock()) }
    qr/got work_complete for unknown handle/,
        "caught process_packet({type => work_complete}) unknown handle";

    $r = { type => "work_exception", blobref => \$h };
    throws_ok { $ts->process_packet($r, $ts->_get_default_sock()) }
    qr/Bogus work_exception from server/,
        "caught process_packet({type => work_exception})";
    $r->{blobref} = \join "\0", ${ $r->{blobref} }, "abc";
    throws_ok { $ts->process_packet($r, $ts->_get_default_sock()) }
    qr/got work_exception for unknown handle/,
        "caught process_packet({type => work_exception}) unknown handle";

    $r = { type => "work_status", blobref => \$h };
    throws_ok { $ts->process_packet($r, $ts->_get_default_sock()) }
    qr/got work_status for unknown handle/,
        "caught process_packet({type => work_status}) unknown handle";

    $r->{type} = $f;
    throws_ok { $ts->process_packet($r, $ts->_get_default_sock()) }
    qr/unimplemented packet type/,
        "caught process_packet({type => $f }) unknown handle";
};

done_testing();

