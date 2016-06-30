use strict;
use warnings;
use Test::More;
use Test::Exception;

my $mn = "Gearman::Util";

use_ok($mn);

no strict "refs";

my @chr = ('a' .. 'z', 'A' .. 'Z', 0 .. 9);

ok(my %cmd = %{"$mn\:\:cmd"});
is(keys(%cmd), 27);

foreach my $n (keys %cmd) {
    my $t = $cmd{$n}->[1];
    my $a = join '', map { @chr[rand @chr] } 0 .. int(rand(20)) + 1;

    is(&{"$mn\:\:cmd_name"}($n), $t, "$mn\:\:cmd($n) = $t");

    is(
        &{"$mn\:\:pack_req_command"}($t),
        join('', "\0REQ", pack("NN", $n, 0), ''),
        "$mn\:\:pack_req_command}($t)"
    );

    is(
        &{"$mn\:\:pack_res_command"}($t),
        join('', "\0RES", pack("NN", $n, 0), ''),
        "$mn\:\:pack_res_command}($t)"
    );

    is(
        &{"$mn\:\:pack_req_command"}($t, $a),
        join('', "\0REQ", pack("NN", $n, length($a)), $a),
        "$mn\:\:pack_req_command}($t, $a)"
    );

    is(
        &{"$mn\:\:pack_res_command"}($t, $a),
        join('', "\0RES", pack("NN", $n, length($a)), $a),
        "$mn\:\:pack_res_command}($t)"
    );
} ## end foreach my $n (keys %cmd)


# throws_ok(sub { &{"$mn\:\:pack_req_command"}() },qr/Bogus type arg of/);
# throws_ok(sub { &{"$mn\:\:pack_res_command"}() },qr/Bogus type arg of/);

done_testing();
