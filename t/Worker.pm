package t::Worker;
use strict;
use warnings;
use base qw/Exporter/;
use Gearman::Worker;
use Proc::Guard;
our @EXPORT = qw/
    new_worker
    /;

sub new_worker {
    my ($job_servers, %func) = @_;
    my $w = Gearman::Worker->new(job_servers => $job_servers);

    while (my ($f, $v) = each(%func)) {
        $w->register_function($f, ref($v) eq "ARRAY" ? @{$v} : $v);
    }

    my $pg = Proc::Guard->new(
        code => sub {
            while (1) {
                $w->work(
                    stop_if => sub {
                        my ($idle, $last_job_time) = @_;
                        return $idle;
                    }
                );
            } ## end while (1)
        }
    );

    return $pg;
} ## end sub new_worker

1;
