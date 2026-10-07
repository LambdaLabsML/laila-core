{
  "targets": [
    {
      "target_name": "laila_loop_pump",
      "sources": ["loop_pump.c"],
      "defines": ["NAPI_VERSION=8"],
      "cflags": ["-O2", "-Wall"],
      "conditions": [
        ["OS=='win'", {"msvs_settings": {"VCCLCompilerTool": {"ExceptionHandling": 1}}}]
      ]
    }
  ]
}
