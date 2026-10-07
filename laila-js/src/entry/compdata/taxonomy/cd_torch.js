/**
 * ``ComputationalData`` subclass for PyTorch ``torch.Tensor`` payloads.
 *
 * The wrapper is only registered when ``torch`` is importable -- in Python
 * the ``torch`` extras (``pip install laila-core[torch]``) are an optional
 * dependency. There is no torch runtime in JavaScript, so this module is the
 * ``torch``-missing branch of the Python module: the class definition is
 * skipped entirely, the registry has no entry for ``torch.Tensor``, and a
 * torch blob read from a pool is rejected by the ``torch`` recovery-code
 * backend (``NotImplementedError``).
 */
export const torch = null;
export const _HAVE_TORCH = false;
