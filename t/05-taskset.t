use strict;
use warnings;

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

# subtest "hook", sub {
#     my $cb = sub { 2 * shift };
#     my $h = "ahook";
#     ok($ts->add_hook($h, $cb), "add_hook($h, ..)");
#     is($ts->{hooks}->{$h}, $cb, "$h is a cb");
#     $ts->run_hook($h, 2, "run_hook($h)");
#     ok($ts->add_hook($h), "add_hook($h, undef)");
#     is($ts->{hooks}->{$h}, undef, "$h undef");
# };

# subtest "cancel", sub {
#     is($ts->{cancelled}, 0);

#     # just in order to test close in cancel sub
#     $ts->{default_sock} = IO::Socket::IP->new();
#     $ts->{loaned_sock}->{x} = IO::Socket::IP->new();

#     $ts->cancel();

#     is($ts->{cancelled},          1,     "cancelled");
#     is($ts->{default_sock},       undef, "default_sock");
#     is(keys(%{ $ts->{waiting} }), 0,     "waiting");
#     is(@{ $ts->{need_handle} },   0,     "need_handle");
#     is($ts->{client},             undef, "client");

#     delete $ts->{loaned_sock}->{x};
# };

# subtest "socket", sub {
#     my $gts = t::Server->new();
#     $gts || plan skip_all => $t::Server::ERROR;

#     my $job_server = $gts->job_servers();
#     $job_server || plan skip_all => "couldn't start ", $gts->bin();

#     my $c = new_ok($cn, [job_servers => [$job_server]]);
#     my $ts = new_ok($mn, [$c]);

#     my @js = @{ $ts->{client}->job_servers() };
#     for (my $i = 0; $i < scalar(@js); $i++) {

#         ok(my $ls = $ts->_get_loaned_sock($js[$i]),
#             "_get_loaned_sock($js[$i])");
#         isa_ok($ls, "IO::Socket::IP");
#         is($ts->_get_hashed_sock($i),
#             $ls, "_get_hashed_sock($i) = _get_loaned_sock($js[$i])");
#     } ## end for (my $i = 0; $i < scalar...)

#     ok($ts->_get_default_sock(),                "_get_default_sock");
#     ok($ts->_ip_port($ts->_get_default_sock()), "_ip_port");
# };

# subtest "task", sub {
#     throws_ok { $ts->_fail_jshandle() } qr/called without shandle/,
#         "caught _fail_jshandle() without shandle";

#     throws_ok { $ts->_fail_jshandle('x') } qr/unknown handle/,
#         "caught _fail_jshandle() unknown shandle";

#     dies_ok { $ts->_wait_for_packet() } "_wait_for_packet() dies";
#     dies_ok { $ts->add_task() } "add_task() dies";
#     my $f = "foo";
#     $ts->{need_handle} = [];
#     $ts->{client} = new_ok($cn, [job_servers => [@js]]);
#     if (!@js) {
#         is($ts->add_task($f), undef, "add_task($f) returns undef");
#     }
#     else {
#         ok($ts->add_task($f), "add_task($f) returns handle");
#         is(scalar(@{ $ts->{need_handle} }), 0);

#         #TODO timeout test
#         is($ts->_wait_for_packet($ts->_get_default_sock(), 1),
#             0, "_wait_for_packet");
#     } ## end else [ if (!@js) ]

#     # is($ts->add_task(qw/a b/), undef, "add_task returns undef");
# };

my $f = "foo";
my $h = "H:localhost:12345";

subtest "process_packet(job_created)", sub {
    plan tests => 7;

    my $sock = $ts->_get_default_sock();
    ok(
        my $task = $ts->client()->_get_task_from_args(
            $f, undef,
            {
                on_fail => sub {
                    my ($m) = shift;
                    is($m, "jshandle fail", "on fail message");
                }
            }
        ),
        "task"
    );

    $ts->{need_handle} = [];
    $ts->{client} = new_ok("Gearman::Client", [job_servers => [@js]]);

    my $type = "job_created";
    my $r = { type => $type, blobref => \$h };

    # job_created
    throws_ok { $ts->process_packet($r, $sock) }
    qr/unexpected $type/, "$type exception";

    $ts->{need_handle} = [$task];
    $ts->{waiting}{$h} = [$task];
    ok($ts->process_packet($r, $sock), "process_packet");

    is(scalar(@{ $ts->{need_handle} }), 0, "need_handle is empty");
    is($ts->{waiting}{$h}, undef, "no waiting{$h}");
};

subtest "process_packet(work_complete)", sub {
    plan tests => 6;
    my $type = "work_complete";
    my $r = { type => $type, blobref => \$h };
    throws_ok { $ts->process_packet($r, $ts->_get_default_sock()) }
    qr/Bogus $type from server/, "caught bogus $type";

    $r->{blobref} = \join "\0", $h, "12345";
    throws_ok { $ts->process_packet($r, $ts->_get_default_sock()) }
    qr/task_list is empty on $type/, "caught task list is empty";

    ok(
        my $task = $ts->client()->_get_task_from_args(
            $f, undef,
            {
                on_complete => sub {
                    my ($blob) = shift;
                    is(${$blob}, "12345", "on complete");
                }
            }
        ),
        "task"
    );

    $ts->{waiting}{$h} = [$task];
    ok($ts->process_packet($r), "process_packet");
    is($ts->{waiting}{$h}, undef, "no waiting{$h}");
};

subtest "process_packet(work_data)", sub {
    plan tests => 6;

    my $type = "work_data";
    my $r = { type => $type, blobref => \$h };

    throws_ok { $ts->process_packet($r, $ts->_get_default_sock()) }
    qr/Bogus $type from server/, "caught bogus $type";

    $r->{blobref} = \join "\0", $h, "abc";

    throws_ok { $ts->process_packet($r, $ts->_get_default_sock()) }
    qr/task_list is empty on $type/, "caught task list is empty";

    ok(
        my $task = $ts->client()->_get_task_from_args(
            $f, undef,
            {
                on_data => sub {
                    my ($blob) = shift;
                    is(${$blob}, "abc", "on data");
                }
            }
        ),
        "task"
    );

    $ts->{waiting}{$h} = [$task];
    ok($ts->process_packet($r), "process_packet");
    is(scalar(@{ $ts->{waiting}{$h} }), 1, "waiting{$h}");
};

subtest "process_packet(work_exception)", sub {
    plan tests => 5;
    my $type = "work_exception";
    my $r = { type => $type, blobref => \$h };

    throws_ok { $ts->process_packet($r, $ts->_get_default_sock()) }
    qr/Bogus $type from server/, "caught bogus $type";

    ok(
        my $task = $ts->client()->_get_task_from_args(
            $f, undef,
            {
                on_exception => sub {
                    my ($blob) = shift;
                    is($blob, "abc", "on exception");
                }
            }
        ),
        "task"
    );
    $r->{blobref} = \join "\0", ${ $r->{blobref} }, "abc";

    $ts->{waiting}{$h} = [$task];
    ok($ts->process_packet($r), "process_packet");
    is(scalar(@{ $ts->{waiting}{$h} }), 1, "waiting{$h}");
};

subtest "process_packet(work_fail)", sub {
    plan tests => 4;

    ok(
        my $task = $ts->client()->_get_task_from_args(
            $f, undef,
            {
                on_fail => sub {
                    my ($m) = shift;
                    is($m, "jshandle fail", "on fail message");
                }
            }
        ),
        "task"
    );

    my $type = "work_fail";
    my $r = { type => $type, blobref => \$h };

    $ts->{waiting}{$h} = [$task];
    ok($ts->process_packet($r), "process_packet");

    is($ts->{waiting}{$h}, undef, "no waiting{$h}");
};

subtest "process_packet(work_status)", sub {
    plan tests => 6;
    my $type = "work_status";
    my $r = { type => $type, blobref => \join "\0", $h, 3, 5 };
    $ts->{waiting}{$h} = [];
    throws_ok { $ts->process_packet($r) }
    qr/got $type for unknown handle/, "caught unknown handle";

    ok(
        my $task = $ts->client()->_get_task_from_args(
            $f, undef,
            {
                on_status => sub {
                    my ($nu, $de) = @_;
                    is($nu, 3);
                    is($de, 5);
                }
            }
        ),
        "task"
    );
    $ts->{waiting}{$h} = [$task];

    ok($ts->process_packet($r), "process_packet");
    is(scalar(@{ $ts->{waiting}{$h} }), 1, "waiting{$h}");
};

subtest "process_packet(unimplemented type)", sub {
    plan tests => 1;
    my $type = $f;
    my $r = { type => $type, blobref => \"x"};
    throws_ok { $ts->process_packet($r) }
    qr/Unimplemented packet type: $f/, "caught unimplemented packet type";
};
done_testing();

