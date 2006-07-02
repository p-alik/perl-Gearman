use 5.008;
use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
              'NAME'=> 'Gearman',
              'VERSION' => '1.00',
              'PREREQ_PM'=> {
                String::CRC32 => 0,
              }, # e.g., Module::Name => 1.1
              AUTHOR     => 'Brad Fitzpatrick <brad@danga.com>',
              );

1;
