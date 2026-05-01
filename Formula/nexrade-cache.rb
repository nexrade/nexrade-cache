# typed: false
# frozen_string_literal: true

class NexradeCache < Formula
  desc "High-performance Redis-compatible in-memory cache server written in Rust"
  homepage "https://github.com/nexrade/nexrade-cache"
  version "0.1.0"

  on_macos do
    on_arm do
      url "https://github.com/nexrade/nexrade-cache/releases/download/v#{version}/nexrade-cache-macos-arm64.tar.gz"
      sha256 "PLACEHOLDER_MACOS_ARM64_SHA256"
    end
  end

  on_linux do
    on_intel do
      url "https://github.com/nexrade/nexrade-cache/releases/download/v#{version}/nexrade-cache-linux-x86_64-musl.tar.gz"
      sha256 "PLACEHOLDER_LINUX_X86_64_SHA256"
    end
    on_arm do
      url "https://github.com/nexrade/nexrade-cache/releases/download/v#{version}/nexrade-cache-linux-arm64-musl.tar.gz"
      sha256 "PLACEHOLDER_LINUX_ARM64_SHA256"
    end
  end

  bottle :unneeded

  def install
    bin.install "nexrade-cache"
    bin.install "nexrade-cli"
    etc.install "nexrade.toml" => "nexrade-cache.toml"
  end

  def post_install
    (var/"nexrade-cache").mkpath
    (var/"log/nexrade-cache").mkpath
  end

  service do
    run          [opt_bin/"nexrade-cache", "--config", "#{etc}/nexrade-cache.toml"]
    keep_alive   true
    working_dir  var/"nexrade-cache"
    log_path     var/"log/nexrade-cache/nexrade-cache.log"
    error_log_path var/"log/nexrade-cache/nexrade-cache-error.log"
  end

  test do
    port = free_port
    config = testpath/"nexrade-cache.toml"
    config.write <<~TOML
      bind = "127.0.0.1"
      port = #{port}
    TOML

    pid = fork { exec bin/"nexrade-cache", "--config", config.to_s }
    sleep 1

    assert_match "PONG", shell_output("#{bin}/nexrade-cli --host 127.0.0.1 --port #{port} PING")
  ensure
    Process.kill("TERM", pid)
    Process.wait(pid)
  end
end
