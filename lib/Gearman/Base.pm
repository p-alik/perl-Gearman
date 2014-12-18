package Gearman::Base;
use strict;
use warnings;

use constant DEFAULT_PORT => 4730;

use fields qw/
    debug
    job_servers
    js_count
    /;

sub new {
    my Gearman::Base $self = shift;
    my (%opts) = @_;
    unless (ref($self)) {
        $self = fields::new($self);
    }
    $self->{job_servers} = [];
    $self->{js_count}    = 0;

    $opts{job_servers} && $self->set_job_servers(@{ $opts{job_servers} });
    $opts{debug} && $self->debug($opts{debug});

    return $self;
} ## end sub new

# getter/setter
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
    my $list = ref $_[0] ? $_[0] : [@_];    # take arrayref or array
    foreach (@$list) {
        $_ .= ':' . Gearman::Base::DEFAULT_PORT unless /:/;
    }
    return $list;
} ## end sub canonicalize_job_servers

sub debug {
    my $self = shift;
    $self->{debug} = shift if @_;
    return $self->{debug} || 0;
}

1;
