"""Run inside the locked transcription environment; decode without a system ffmpeg."""

import platform
import sys

MAC_MODEL = ('mlx-community/whisper-large-v3-turbo', 'a4aaeec0636e6fef84abdcbe3544cb2bf7e9f6fb')
LINUX_MODEL = ('mobiuslabsgmbh/faster-whisper-large-v3-turbo', '0a363e9161cbc7ed1431c9597a8ceaf0c4f78fcf')


def uses_mlx():
    return platform.system() == 'Darwin' and platform.machine() == 'arm64'


def model_path(setup=False):
    from huggingface_hub import snapshot_download

    repo, revision = MAC_MODEL if uses_mlx() else LINUX_MODEL
    return snapshot_download(repo_id=repo, revision=revision, local_files_only=not setup,
                             allow_patterns=['config.json', 'weights.*', 'model.bin', 'tokenizer.json',
                                             'preprocessor_config.json', 'vocabulary.*'])


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


def main():
    if sys.argv[1] == '--setup':
        model_path(setup=True)
        return
    model = model_path()
    samples = decode(sys.argv[1])
    if uses_mlx():
        import mlx_whisper
        text = mlx_whisper.transcribe(samples, path_or_hf_repo=model)['text']
    else:
        from faster_whisper import WhisperModel
        segments, _ = WhisperModel(model, device='cpu', compute_type='int8').transcribe(samples)
        text = ' '.join(segment.text for segment in segments)
    print(text)


if __name__ == '__main__':
    main()
