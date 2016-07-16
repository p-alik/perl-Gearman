package Gearman::Objects;
use version;
$Gearman::Objects::VERSION = qv("2.001.001_1");

use strict;
use warnings;

use constant DEFAULT_PORT => 4730;

use IO::Socket::INET ();
use IO::Socket::SSL  ();

use fields qw/
    debug
    job_servers
    js_count
    prefix
    use_ssl
    /;

sub new {
    my Gearman::Objects $self = shift;
    my (%opts) = @_;
    unless (ref($self)) {
        $self = fields::new($self);
    }
    $self->{job_servers} = [];
    $self->{js_count}    = 0;

    $opts{job_servers}
        && $self->set_job_servers(
        ref($opts{job_servers})
        ? @{ $opts{job_servers} }
        : [$opts{job_servers}]
        );

    $self->debug($opts{debug});
    $self->prefix($opts{prefix});
    $self->use_ssl($opts{use_ssl});

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
        $_ .= ':' . Gearman::Objects::DEFAULT_PORT unless /:/;
    }
    return $list;
} ## end sub canonicalize_job_servers

sub debug {
    return shift->_property("debug", @_);
}

sub prefix {
    return shift->_property("prefix", @_);
}

sub use_ssl {
    return shift->_property("use_ssl", @_);
}

=head2 socket($host_port, [$timeout])

depends on C<use_ssl> 
prepare L<IO::Socket::INET>
or L<IO::Socket::SSL>

=over

=item

C<$host_port> peer address

=item

C<$timeout> default: 1

=back

B<return> depends on C<use_ssl> IO::Socket::(INET|SSL) on success

=cut

sub socket {
    my ($self, $pa, $t) = @_;

    my $sc = join "::", "IO::Socket", $self->use_ssl() ? "SSL" : "INET";
    return $sc->new(
        PeerAddr => $pa,
        Timeout  => $t || 1
    );
} ## end sub socket

#
# _property($name, [$value])
# set/get
sub _property {
    my $self = shift;
    my $name = shift;
    $name || return;
    if (@_) {
        $self->{$name} = shift;
    }

    return $self->{$name};
} ## end sub _property

1;
