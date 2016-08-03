package Gearman::Objects;
use version;
$Gearman::Objects::VERSION = qv("1.130.002");

use strict;
use warnings;

=head1 NAME

Gearman::Objects - a parrent class for L<Gearman::Client> and L<Gearman::Worker>

=head1 METHODS

=cut

use constant DEFAULT_PORT => 4730;

use fields qw/
    debug
    job_servers
    js_count
    prefix
    /;

sub new {
    my Gearman::Objects $self = shift;
    my (%opts) = @_;
    unless (ref($self)) {
        $self = fields::new($self);
    }
    $self->{job_servers} = [];
    $self->{js_count}    = 0;
    $self->{prefix}      = undef;

    $opts{job_servers}
        && $self->set_job_servers(
        ref($opts{job_servers})
        ? @{ $opts{job_servers} }
        : [$opts{job_servers}]
        );
    $opts{debug}  && $self->debug($opts{debug});
    $opts{prefix} && $self->prefix($opts{prefix});

    return $self;
} ## end sub new

=head2 job_servers([$js])

getter/setter

C<$js> may be an array reference or scalar

=cut

sub job_servers {
    my ($self) = shift;
    (@_) && $self->set_job_servers(@_);

    return wantarray ? @{ $self->{job_servers} } : $self->{job_servers};
} ## end sub job_servers

sub set_job_servers {
    my $self = shift;
    my $list = $self->canonicalize_job_servers(@_);

    $self->{js_count} = scalar @$list;
    return $self->{job_servers} = $list;
} ## end sub set_job_servers

sub canonicalize_job_servers {
    my ($self) = shift;
    # take arrayref or array
    my $list = ref $_[0] ? $_[0] : [@_];
    foreach (@$list) {
        $_ .= ':' . Gearman::Objects::DEFAULT_PORT unless /:/;
    }
    return $list;
} ## end sub canonicalize_job_servers

sub debug {
    my $self = shift;
    $self->{debug} = shift if @_;
    return $self->{debug} || 0;
}

=head2 prefix([$prefix])

getter/setter

=cut
sub prefix {
    my $self = shift;
    if (@_) {
        $self->{prefix} = shift;
    }
    return $self->{prefix};
} ## end sub prefix

1;
