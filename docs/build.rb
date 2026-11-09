#!/usr/bin/env ruby
# Build the customer guide without compiling or running the connector.
require 'fileutils'
require 'rbconfig'

gem 'asciidoctor', '= 2.0.26'

root = File.expand_path('..', __dir__)
source = File.join(__dir__, 'guide/src/docs/asciidoc')
output = File.join(root, 'build/site')
readme = File.read(File.join(root, 'README.adoc'))
metadata = %w[owner name version].to_h do |key|
  values = readme.scan(/^:project-#{key}:\s+(\S+)\s*$/).flatten
  abort "Expected exactly one project-#{key} in README.adoc" unless values.length == 1
  [key, values.first]
end
abort 'Expected a stable documentation version' unless metadata['version'].match?(/\A(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\z/)

FileUtils.rm_rf(output)
FileUtils.mkdir_p(output)
FileUtils.cp_r(File.join(__dir__, 'guide/src/docs/resources/images'), output)
File.write(File.join(output, '.nojekyll'), '')

attributes = {
  'includedir' => source,
  'imagesdir' => 'images',
  'project-title' => 'Redis Kafka Connector',
  'project-author' => 'Julien Ruaux',
  'project-version' => metadata['version'],
  'project-name' => metadata['name'],
  'project-scm' => "https://github.com/#{metadata['owner']}/#{metadata['name']}",
  'github-owner' => metadata['owner'],
  'github-repo' => metadata['name'],
  'toc' => 'left',
  'sectanchors' => ''
}
arguments = ['--failure-level', 'WARN', '--safe-mode', 'safe', '--base-dir', root,
             '--out-file', File.join(output, 'index.html'), '--section-numbers']
attributes.each { |key, value| arguments.concat(['--attribute', "#{key}=#{value}"]) }
exec RbConfig.ruby, Gem.bin_path('asciidoctor', 'asciidoctor', '= 2.0.26'), *arguments,
     File.join(source, 'index.adoc')
