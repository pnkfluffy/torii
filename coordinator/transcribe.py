"""Run the selected speech model inside its locked transcription environment."""

import argparse
from contextlib import redirect_stdout
from pathlib import Path
import sys

if __package__:
    from . import host_os
else:
    import host_os

MAC_MODEL = ('mlx-community/whisper-large-v3-turbo', 'a4aaeec0636e6fef84abdcbe3544cb2bf7e9f6fb')
LINUX_MODEL = ('mobiuslabsgmbh/faster-whisper-large-v3-turbo', '0a363e9161cbc7ed1431c9597a8ceaf0c4f78fcf')
PARAKEET_MODEL = ('mlx-community/parakeet-tdt-0.6b-v3', 'ed2b7e8c15f9aaa0b5772e2efb986255eaef7e15')


def model_path(setup=False, repair=False, backend='parakeet'):
    from huggingface_hub import snapshot_download

    if backend == 'parakeet':
        repo, revision = PARAKEET_MODEL
        required = ('config.json', 'model.safetensors')
    else:
        (repo, revision), required = host_os.apple_silicon_choice(
            (MAC_MODEL, ('config.json', 'weights.safetensors')),
            (LINUX_MODEL, ('config.json', 'model.bin', 'tokenizer.json', 'vocabulary.json')))
    path = snapshot_download(repo_id=repo, revision=revision, local_files_only=not setup,
                             force_download=repair, token=False,
                             allow_patterns=list(required) + ['preprocessor_config.json', 'README.md',
                                                               'LICENSE*', 'NOTICE*'])
    if any(not (Path(path) / name).is_file() or (Path(path) / name).stat().st_size == 0 for name in required):
        raise RuntimeError('pinned transcription model snapshot is incomplete')
    return path


def decode(path):
    import av
    import numpy as np

    resampler = av.AudioResampler(format='fltp', layout='mono', rate=16000)
    frames = []
    with av.open(path) as audio:
        for frame in audio.decode(audio=0):
            frames.extend(item.to_ndarray().ravel() for item in resampler.resample(frame))
        frames.extend(item.to_ndarray().ravel() for item in resampler.resample(None))
    if not frames:
        raise RuntimeError('audio has no samples')
    return np.concatenate(frames)


def _whisper_mlx(samples, model):
    import mlx_whisper

    return mlx_whisper.transcribe(samples, path_or_hf_repo=model)['text']


def _whisper_cpu(samples, model):
    from faster_whisper import WhisperModel

    segments, _ = WhisperModel(model, device='cpu', compute_type='int8').transcribe(samples)
    return ' '.join(segment.text for segment in segments)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--backend', choices=('parakeet', 'whisper'), default='parakeet')
    parser.add_argument('audio', nargs='?')
    action = parser.add_mutually_exclusive_group()
    action.add_argument('--setup', action='store_true')
    action.add_argument('--repair', action='store_true')
    action.add_argument('--check', action='store_true')
    args = parser.parse_args()
    if args.setup or args.repair:
        model_path(setup=True, repair=args.repair, backend=args.backend)
        return
    if args.check:
        model_path(backend=args.backend)
        return
    if not args.audio:
        parser.error('audio path is required')
    model = model_path(backend=args.backend)
    with redirect_stdout(sys.stderr):
        if args.backend == 'parakeet':
            import mlx.core as mx
            from parakeet_mlx import from_pretrained
            text = from_pretrained(model, dtype=mx.bfloat16).transcribe(
                args.audio, chunk_duration=120.0, overlap_duration=15.0).text
        else:
            samples = decode(args.audio)
            text = host_os.apple_silicon_choice(_whisper_mlx, _whisper_cpu)(samples, model)
    print(text)


if __name__ == '__main__':
    main()
