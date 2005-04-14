
package Gearman::Util;
use strict;

# I: to jobserver
# O: out of job server
# W: worker
# C: client of job server
# J : jobserver
our %cmd = (
            1 =>  [ 'I', "can_do" ],     # from W
            2 =>  [ 'I', "cant_do" ],    # from W
            3 =>  [ 'I', "reset_abilities" ],  # from W
            4 =>  [ 'I', "pre_sleep" ],  # from W
            5 =>  [ 'I', "disconnect" ], # from W

            6 =>  [ 'O', "noop" ],        # J->W (wakeup)
            7 =>  [ 'I', "submit_job" ],  # C->J
            18 => [ 'I', "submit_job_bg" ],  # C->J  FUNC[0]UNIQ[0]ARGS

            8 =>  [ 'O', "job_created" ], # J->C
            9 =>  [ 'I', "grab_job" ],    # W->J
            10 => [ 'O', "no_job" ],      # J->W
            11 => [ 'O', "job_assign" ],  # J->W

            12 => [ 'IO',  "work_status" ],   # W->J/C: HANDLE[0]NUMERATOR[0]DENOMINATOR
            13 => [ 'IO',  "work_complete" ], # W->J/C: HANDLE[0]RES
            14 => [ 'IO',  "work_fail" ],     # W->J/C: HANDLE

            15 => [ 'I',  "get_status" ],  # C->J
            16 => [ 'I',  "echo_req" ],    # ?->J
            17 => [ 'O',  "echo_res" ],    # J->?

            19 => [ 'O',  "error" ],       # J->?
            );

our %num;  # name -> num
while (my ($num, $ary) = each %cmd) {
    die if $num{$ary->[1]};
    $num{$ary->[1]} = $num;
}

sub cmd_name {
    my $num = shift;
    my $c = $cmd{$num};
    return $c ? $c->[1] : undef;
}

sub pack_req_command {
    my $type_arg = shift;
    my $type = int($type_arg) || $num{$type_arg};
    die "Bogus type arg of '$type_arg'" unless $type;
    my $len = length($_[0]);
    return "\0REQ" . pack("NN", $type, $len) . $_[0];
}

sub pack_res_command {
    my $type_arg = shift;
    my $type = int($type_arg) || $num{$type_arg};
    die "Bogus type arg of '$type_arg'" unless $type;
    my $len = length($_[0]);
    return "\0RES" . pack("NN", $type, $len) . $_[0];
}

# returns undef on closed socket or malformed packet
sub read_res_packet {
    my $sock = shift;
    my $err_ref = shift;

    my $buf;
    my $rv;

    my $err = sub {
        my $code = shift;
        $$err_ref = $code if ref $err_ref;
        return undef;
    };

    $rv = read($sock, $buf, 4);
    return $err->("malformed_magic") unless $rv == 4 && $buf eq "\0RES";

    return $err->("malformed_typelen") unless read($sock, $buf, 8) == 8;
    my ($type, $len) = unpack("NN", $buf);

    $rv = read($sock, $buf, $len);
    return $err->("short_body") unless $rv == $len;

    my $type = $cmd{$type};
    return $err->("bogus_command") unless $type;
    return $err->("bogus_command_type") unless index($type->[0], "O") != -1;

    return {
        'type' => $type->[1],
        'len' => $len,
        'blobref' => \$buf,
    };
}

1;
