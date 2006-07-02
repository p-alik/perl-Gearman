
package Gearman::JobStatus;

use strict;

sub new {
    my ($class, $known, $running, $nu, $de) = @_;
    undef $nu unless length($nu);
    undef $de unless length($de);
    my $self = [ $known, $running, $nu, $de ];
    bless $self;
    return $self;
}

sub known { my $self = shift; return $self->[0]; }
sub running { my $self = shift; return $self->[1]; }
sub progress { my $self = shift; return defined $self->[2] ? [ $self->[2], $self->[3] ] : undef; }
sub percent { my $self = shift; return (defined $self->[2] && $self->[3]) ? ($self->[2] / $self->[3]) : undef; }

1;
