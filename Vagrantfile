Vagrant.configure("2") do |config|

  config.vm.box = "ubuntu/trusty64"
  config.vm.hostname = "snowplow"
  config.ssh.forward_agent = true

  config.vm.provider :virtualbox do |vb|
    vb.name = Dir.pwd().split("/")[-1] + "-" + Time.now.to_f.to_i.to_s
    vb.customize ["modifyvm", :id, "--natdnshostresolver1", "on"]
    vb.customize [ "guestproperty", "set", :id, "--timesync-threshold", 10000 ]
    # Scala is memory-hungry
    vb.memory = 5120
  end

  config.vm.provision :shell do |sh|
    sh.path = "vagrant/up.bash"
  end

  # Requires Vagrant 1.7.0+
  config.push.define "assetsync", strategy: "local-exec" do |push|
    push.script = "vagrant/push/sync-hosted-assets.bash"
  end
  config.push.define "kinesis", strategy: "local-exec" do |push|
    push.script = "vagrant/push/publish-kinesis-release.bash"
  end

end
