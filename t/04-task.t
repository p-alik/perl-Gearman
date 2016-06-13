use strict;
use warnings;

use Storable;
use Test::More;
use Test::Exception;

use_ok("Gearman::Client");
use_ok("Gearman::Taskset");

my $mn = "Gearman::Task";
use_ok($mn);

can_ok(
    $mn, qw/
        add_hook
        complete
        exception
        fail
        final_fail
        func
        handle
        hash
        is_finished
        mode
        pack_submit_packet
        run_hook
        set_on_post_hooks
        status
        taskset
        timeout
        wipe
        /
);

my ($f, $arg) = qw/
    foo
    bar
    /;

my %opt = (
    uniq          => rand(10),
    on_complete   => 1,
    on_fail       => 2,
    on_exception  => 3,
    on_retry      => undef,
    on_status     => 4,
    retry_count   => 6,
    try_timeout   => 7,
    high_priority => 1,
    background    => 1,
    timeout       => int(rand(10)),
);

throws_ok { $mn->new($f, \$arg, { $f => 1 }) } qr/Unknown option/,
    "caught unknown option exception";

my $t = new_ok($mn, [$f, \$arg, {%opt}]);

is($t->func, $f, "func");

is(${ $t->{argref} }, $arg, "argref");

foreach (keys %opt) {
    is($t->can($_) ? $t->$_ : $t->{$_}, $opt{$_}, $_);
}

is($t->{$_}, 0, $_) for qw/
    is_finished
    retries_done
    /;

subtest "mode", sub {
    $t->{background}    = undef;
    $t->{high_priority} = 0;
    is($t->mode, "submit_job", "submit_job");
    $t->{high_priority} = 1;
    is($t->mode, "submit_job_high", "submit_job_high");

    is($t->{background} = 1, 1, "background");
    is($t->mode, "submit_job_high_bg", "submit_job_high_bg");
    $t->{high_priority} = 0;
    is($t->mode, "submit_job_bg", "submit_job_bg");

    ok($t->pack_submit_packet(new_ok("Gearman::Client", [prefix => $f])),
        "pack_submit_packet");
};
my @h = qw/
    on_post_hooks
    on_complete
    on_fail
    on_retry
    on_status
    hooks
    /;

subtest "wipe", sub {

    $t->{$_} = 1 for @h;

    $t->wipe();

    is($t->{$_}, undef, $_) for @h;
};

subtest "hook", sub {
    my $cb = sub { 2 * shift };
    ok($t->add_hook($f, $cb));
    is($t->{hooks}->{$f}, $cb);
    $t->run_hook($f, 2);
    ok($t->add_hook($f));
    is($t->{hooks}->{$f}, undef);
};

subtest "taskset", sub {
    is($t->taskset, undef, "taskset");
    throws_ok { $t->taskset($f) } qr/not an instance of Gearman::Taskset/,
        "caught taskset($f) exception";

    my $c = new_ok("Gearman::Client");
    my $ts = new_ok("Gearman::Taskset", [$c]);
    ok($t->taskset($ts));
    is($t->taskset(), $ts);
    is($t->hash(),    $t->hash());

    $t->{uniq} = '-';
    is($t->taskset(), $ts);
    is($t->hash(),    $t->hash());
};

subtest "fail", sub {
    $t->{is_finished} = 1;
    is($t->fail(), undef);

    $t->{is_finished}  = undef;
    $t->{on_retry}     = sub { is(shift, $t->{retry_count}, "on_retry") };
    $t->{retries_done} = 0;
    $t->{retry_count}  = 1;
    $t->fail($f);
    is($t->{retries_done}, $t->{retry_count}, "retries_done = retry_count");

    $t->{is_finished} = undef;
    $t->{on_fail} = sub { is(shift, $f, "on_fail") };
    $t->final_fail($f);
    is($t->{is_finished}, $f);

    is($t->{$_}, undef, $_) for @h;
};

subtest "exception", sub {
    my $exc = Storable::freeze(\$f);
    $t->{on_exception} = sub { is(shift, $f) };
    is($t->exception(\$exc), undef);
    pass("x");
};

subtest "complete", sub {
    $t->{is_finished} = undef;
    $t->{on_complete} = sub { is(shift, $f) };
    $t->complete($f);
    is($t->{is_finished}, "complete");
};

subtest "status", sub {
    $t->{is_finished} = undef;
    $t->{on_status} = sub { is(shift, $f), is(shift, $arg) };
    $t->status($f, $arg);
};

subtest "handle", sub {
    ok($t->handle($f));
    is($t->{handle}, $f);
};

done_testing();
