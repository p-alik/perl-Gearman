use strict;
use warnings;
use Test::More;
use Test::Exception;

use_ok("Gearman::Task");
can_ok(
    "Gearman::Task", qw/
        run_hook
        add_hook
        is_finished
        taskset
        hash
        _hashfunc
        pack_submit_packet
        fail
        final_fail
        exception
        complete
        status
        handle
        set_on_post_hooks
        wipe
        func
        timeout
        mode
        /
);

my ($f, $arg, $to) = (qw/foo bar/, int(rand(10)));

#my $to = int(rand(10));

my $t = new_ok("Gearman::Task", [$f, \$arg, { timeout => $to }]);
is($t->func,          $f,   "func");
is(${ $t->{argref} }, $arg, "argref");
is($t->timeout,       $to,  "timeout");

is($t->{$_}, 0, $_) for qw/
    is_finished
    retry_count
    /;

is($t->taskset, undef, "taskset");
throws_ok { $t->taskset($f) } qr/not an instance of Gearman::Taskset/,
    "cought taskset($f) exception";
is($t->{background}, undef,        "!background");
is($t->mode,         "submit_job", "submit_job");
is($t->{high_priority} = 1, 1, "high_priority");
is($t->mode, "submit_job_high", "submit_job_high");

is($t->{background} = 1, 1, "background");
is($t->mode, "submit_job_high_bg", "submit_job_high_bg");
is($t->{high_priority} = 0, 0, "!high_priority");
is($t->mode, "submit_job_bg", "submit_job_bg");

done_testing();
