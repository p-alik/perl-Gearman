package Gearman::ResponseParser::Taskset;
use version;
$Gearman::ResponseParser::Taskset::VERSION = qv("1.130.002");

use strict;
use warnings;

use base 'Gearman::ResponseParser';

sub new {
    my ($class, %opts) = @_;
    my $ts = delete $opts{taskset};
    ref($ts) eq "Gearman::Taskset"
        || die "provided taskset argument is not a Gearman::Taskset reference";

    my $self = $class->SUPER::new(%opts);
    $self->{_taskset} = $ts;
    return $self;
} ## end sub new

sub on_packet {
    my ($self, $packet, $parser) = @_;
    $self->{_taskset}->_process_packet($packet, $parser->source);
}

sub on_error {
    my ($self, $errmsg) = @_;
    die "ERROR: $errmsg\n";
}

1;
