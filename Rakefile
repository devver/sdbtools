require 'rubygems'
require 'rake'

begin
  require 'jeweler'
  Jeweler::Tasks.new do |gem|
    gem.name = "sdbtools"
    gem.summary = %Q{A high-level OO interface to Amazon SimpleDB}
    gem.description = <<END
SDBTools layers a higher-level OO interface on top of RightAWS, as well as
providing some command-line utilities for working with SimpleDB.
END
    gem.email = "devs@devver.net"
    gem.homepage = "http://github.com/devver/sdbtools"
    gem.authors = ["Avdi Grimm"]
    gem.add_dependency "aws",         "~> 2.1"
    gem.add_dependency 'main',        '~> 4.2'
    gem.add_dependency 'fattr',       '~> 2.1'
    gem.add_dependency 'arrayfields', '~> 4.7'
    gem.add_development_dependency "rspec", ">= 1.2.9"
  end
  Jeweler::GemcutterTasks.new
rescue LoadError
  puts "Jeweler (or a dependency) not available. Install it with: gem install jeweler"
end

require 'spec/rake/spectask'
Spec::Rake::SpecTask.new(:spec) do |spec|
  spec.libs << 'lib' << 'spec'
  spec.spec_files = FileList['spec/**/*_spec.rb']
end

Spec::Rake::SpecTask.new(:rcov) do |spec|
  spec.libs << 'lib' << 'spec'
  spec.pattern = 'spec/**/*_spec.rb'
  spec.rcov = true
end

task :spec => :check_dependencies

task :default => :spec

require 'rake/rdoctask'
Rake::RDocTask.new do |rdoc|
  version = File.exist?('VERSION') ? File.read('VERSION') : ""

  rdoc.rdoc_dir = 'rdoc'
  rdoc.title = "sdbtools #{version}"
  rdoc.rdoc_files.include('README*')
  rdoc.rdoc_files.include('lib/**/*.rb')
end
